package com.oms.api;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.readmodel.view.OrderEventListener;
import com.oms.readmodel.view.OrderView;
import com.oms.readmodel.view.ViewServerReadModel;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Undertow-based query server on port 8081.
 *
 * <p>Implements OrderEventListener so ViewServerReadModel can push updates directly.
 * WebSockets.sendText() is thread-safe — safe to call from the AgentRunner thread.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>GET /orders                    — all orders snapshot
 *   <li>GET /orders/{orderId}          — single order by id
 *   <li>GET /accounts/{accountId}/orders — orders filtered by account
 *   <li>WS  /ws/orders                — snapshot on connect + live push
 *   <li>GET /openapi.yaml              — raw OpenAPI spec
 *   <li>GET /docs                      — Swagger UI HTML
 * </ul>
 *
 * TODO(POC): add back-pressure handling if WS write buffers fill under load.
 * TODO(POC): add authentication/authorisation before production use.
 */
public class OrderQueryServer implements OrderEventListener {

    private static final Log log = LogFactory.getLog(OrderQueryServer.class);

    private static final int PORT = 8081;

    private final ObjectMapper mapper = new ObjectMapper();

    // CopyOnWriteArraySet: connects/disconnects are rare; broadcast loop iterates without locking.
    private final Set<WebSocketChannel> wsChannels = new CopyOnWriteArraySet<>();

    private Undertow undertow;
    private ViewServerReadModel viewModel;

    /**
     * Builds and starts the Undertow server.
     * Call AFTER viewModel.setListener(this) and BEFORE AgentRunner.startOnThread().
     */
    public void start(final ViewServerReadModel vm) {
        this.viewModel = vm;

        final PathHandler root = new PathHandler()
                .addExactPath("/orders",             new AllOrdersHandler())
                .addPrefixPath("/orders/",           new SingleOrderHandler())
                .addPrefixPath("/accounts/",         new AccountOrdersHandler())
                .addExactPath("/ws/orders",          buildWsHandler())
                .addExactPath("/openapi.yaml",       new StaticResourceHandler("openapi.yaml", "text/yaml"))
                .addExactPath("/docs",               new StaticResourceHandler("swagger-ui.html", "text/html"));

        undertow = Undertow.builder()
                .addHttpListener(PORT, "0.0.0.0", withCors(root))
                .build();
        undertow.start();
        log.info().append("[query-server] started on port ").append(PORT).commit();
    }

    public void stop() {
        if (undertow != null) {
            undertow.stop();
            log.info().append("[query-server] stopped").commit();
        }
    }

    // ── OrderEventListener ────────────────────────────────────────────────────

    /**
     * Called from the ViewServer AgentRunner thread after each orders.put().
     * Serialises to JSON and broadcasts to all open WebSocket channels.
     * WebSockets.sendText() is non-blocking and thread-safe.
     */
    @Override
    public void onOrderUpdate(final long orderId, final OrderView view) {
        if (wsChannels.isEmpty()) return;
        try {
            final String json = mapper.writeValueAsString(OrderDto.from(view));
            for (final WebSocketChannel ch : wsChannels) {
                if (ch.isOpen()) {
                    WebSockets.sendText(json, ch, null);
                }
            }
        } catch (Exception e) {
            log.warn().append("[query-server] WS broadcast error: ").append(e.getMessage()).commit();
        }
    }

    // ── WebSocket handler ─────────────────────────────────────────────────────

    private WebSocketProtocolHandshakeHandler buildWsHandler() {
        return new WebSocketProtocolHandshakeHandler(
                (WebSocketHttpExchange exchange, WebSocketChannel channel) -> {
                    wsChannels.add(channel);
                    log.info().append("[query-server] WS client connected, total=")
                            .append(wsChannels.size()).commit();

                    // Send full snapshot immediately on connect
                    try {
                        final List<OrderDto> snapshot = viewModel.getOrders().values().stream()
                                .map(OrderDto::from)
                                .toList();
                        WebSockets.sendText(mapper.writeValueAsString(snapshot), channel, null);
                    } catch (Exception e) {
                        log.warn().append("[query-server] snapshot send error: ").append(e.getMessage()).commit();
                    }

                    // No-op receiver — clients don't send messages in this POC
                    channel.getReceiveSetter().set(new AbstractReceiveListener() {});
                    channel.addCloseTask(ch -> {
                        wsChannels.remove(ch);
                        log.info().append("[query-server] WS client disconnected, total=")
                                .append(wsChannels.size()).commit();
                    });
                    channel.resumeReceives();
                });
    }

    // ── REST handlers ─────────────────────────────────────────────────────────

    private class AllOrdersHandler implements HttpHandler {
        @Override
        public void handleRequest(final HttpServerExchange exchange) throws Exception {
            if (!Methods.GET.equals(exchange.getRequestMethod())) {
                exchange.setStatusCode(405);
                return;
            }
            final Collection<OrderView> views = viewModel.getOrders().values();
            final List<OrderDto> dtos = views.stream().map(OrderDto::from).toList();
            sendJson(exchange, mapper.writeValueAsString(dtos));
        }
    }

    private class SingleOrderHandler implements HttpHandler {
        @Override
        public void handleRequest(final HttpServerExchange exchange) throws Exception {
            if (!Methods.GET.equals(exchange.getRequestMethod())) {
                exchange.setStatusCode(405);
                return;
            }
            // Path: /orders/{orderId} — extract the tail after "/orders/"
            final String tail = exchange.getRelativePath().replaceFirst("^/", "");
            final long orderId;
            try {
                orderId = Long.parseLong(tail);
            } catch (NumberFormatException e) {
                exchange.setStatusCode(400);
                sendJson(exchange, "{\"error\":\"invalid orderId\"}");
                return;
            }

            final OrderView view = viewModel.getOrders().get(orderId);
            if (view == null) {
                exchange.setStatusCode(404);
                sendJson(exchange, "{\"error\":\"not found\"}");
                return;
            }
            sendJson(exchange, mapper.writeValueAsString(OrderDto.from(view)));
        }
    }

    private class AccountOrdersHandler implements HttpHandler {
        @Override
        public void handleRequest(final HttpServerExchange exchange) throws Exception {
            if (!Methods.GET.equals(exchange.getRequestMethod())) {
                exchange.setStatusCode(405);
                return;
            }
            // Path: /accounts/{accountId}/orders — strip leading "/" then split
            final String relative = exchange.getRelativePath().replaceFirst("^/", "");
            // relative is like "42/orders"
            final String[] parts = relative.split("/");
            if (parts.length < 2 || !"orders".equals(parts[1])) {
                exchange.setStatusCode(404);
                return;
            }
            final long accountId;
            try {
                accountId = Long.parseLong(parts[0]);
            } catch (NumberFormatException e) {
                exchange.setStatusCode(400);
                sendJson(exchange, "{\"error\":\"invalid accountId\"}");
                return;
            }

            final List<OrderDto> dtos = viewModel.getOrders().values().stream()
                    .filter(v -> v.accountId == accountId)
                    .map(OrderDto::from)
                    .toList();
            sendJson(exchange, mapper.writeValueAsString(dtos));
        }
    }

    private static class StaticResourceHandler implements HttpHandler {
        private final byte[] content;
        private final String contentType;

        StaticResourceHandler(final String resourceName, final String contentType) {
            this.contentType = contentType;
            byte[] loaded;
            try (InputStream is = StaticResourceHandler.class.getClassLoader()
                    .getResourceAsStream(resourceName)) {
                loaded = (is != null) ? is.readAllBytes() : new byte[0];
            } catch (IOException e) {
                loaded = new byte[0];
            }
            this.content = loaded;
        }

        @Override
        public void handleRequest(final HttpServerExchange exchange) {
            if (!Methods.GET.equals(exchange.getRequestMethod())) {
                exchange.setStatusCode(405);
                return;
            }
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, contentType);
            exchange.getResponseSender().send(
                    java.nio.ByteBuffer.wrap(content));
        }
    }

    /**
     * Wraps any handler with permissive CORS headers so Swagger UI (port 8081) can call
     * the ingress (port 8080) and the query endpoints without browser blocking.
     * Also handles OPTIONS pre-flight requests so the browser doesn't wait for a real handler.
     * TODO(POC): tighten origin whitelist before production.
     */
    private static HttpHandler withCors(final HttpHandler next) {
        return exchange -> {
            exchange.getResponseHeaders()
                    .put(new HttpString("Access-Control-Allow-Origin"),  "*")
                    .put(new HttpString("Access-Control-Allow-Methods"), "GET, POST, DELETE, PATCH, OPTIONS")
                    .put(new HttpString("Access-Control-Allow-Headers"), "Content-Type, Accept");
            if (Methods.OPTIONS.equals(exchange.getRequestMethod())) {
                exchange.setStatusCode(204);
                exchange.endExchange();
                return;
            }
            next.handleRequest(exchange);
        };
    }

    private static void sendJson(final HttpServerExchange exchange, final String json) {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(json, StandardCharsets.UTF_8);
    }
}
