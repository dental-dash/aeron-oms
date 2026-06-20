package com.oms.fix.client;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import uk.co.real_logic.artio.decoder.ExecutionReportDecoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionAcquireHandler;
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Map;

/**
 * M9: Spring-managed {@link SessionAcquireHandler} for the FIX initiator.
 *
 * <p>Dispatches incoming {@code ExecutionReport (35=8)} messages to the SSE emitter
 * keyed by {@code clOrdId} (reflected back as FIX {@code orderID} by {@code FixExecReportBridge}).
 *
 * <p>{@link FixInitiatorService} is injected {@code @Lazy} to break the mutual
 * dependency: this bean is constructed first (FixInitiatorService needs it),
 * so the service's proxy is provided at construction time and resolved on first use.
 */
@Component
public class FixClientSessionHandler implements SessionAcquireHandler
{
    private final EmitterRegistry      emitterRegistry;
    private final FixInitiatorService  initiatorService;

    public FixClientSessionHandler(final EmitterRegistry emitterRegistry,
                                   @Lazy final FixInitiatorService initiatorService)
    {
        this.emitterRegistry  = emitterRegistry;
        this.initiatorService = initiatorService;
    }

    @Override
    public SessionHandler onSessionAcquired(final Session session, final SessionAcquiredInfo acquiredInfo)
    {
        final CompositeKey key = session.compositeKey();
        System.out.printf("[Client] Session acquired: id=%d local=%s remote=%s%n",
            session.id(), key.localCompId(), key.remoteCompId());
        return new LoggingSessionHandler(emitterRegistry, initiatorService);
    }

    // ── Inner session handler ────────────────────────────────────────────────

    private static final class LoggingSessionHandler implements SessionHandler
    {
        private final EmitterRegistry     emitterRegistry;
        private final FixInitiatorService initiatorService;

        // Pre-allocated — used only on the library poll thread (single-threaded).
        private final ExecutionReportDecoder execReportDecoder = new ExecutionReportDecoder();
        private final MutableAsciiBuffer     asciiWrapper      = new MutableAsciiBuffer();

        LoggingSessionHandler(final EmitterRegistry emitterRegistry,
                              final FixInitiatorService initiatorService)
        {
            this.emitterRegistry  = emitterRegistry;
            this.initiatorService = initiatorService;
        }

        @Override
        public void onSessionStart(final Session session)
        {
            final CompositeKey key = session.compositeKey();
            System.out.printf("[Client] Logon complete: local=%s remote=%s%n",
                key.localCompId(), key.remoteCompId());
            // Notify the service that the session is active so it can drain the send queue.
            initiatorService.onSessionReady(session);
        }

        @Override
        public Action onMessage(
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final int libraryId,
                final Session session,
                final int sequenceIndex,
                final long messageType,
                final long timestampInNs,
                final long position,
                final OnMessageInfo messageInfo)
        {
            if (messageType == ExecutionReportDecoder.MESSAGE_TYPE)
            {
                asciiWrapper.wrap(buffer, offset, length);
                execReportDecoder.decode(asciiWrapper, 0, length);

                final String clOrdId = execReportDecoder.orderIDAsString();
                System.out.printf("[FIX-Client] ExecReport: clOrdId=%s execType=%c ordStatus=%c symbol=%s side=%c%n",
                    clOrdId,
                    execReportDecoder.execType(),
                    execReportDecoder.ordStatus(),
                    execReportDecoder.symbolAsString(),
                    execReportDecoder.side());

                final SseEmitter emitter = emitterRegistry.get(clOrdId);
                if (emitter != null)
                {
                    try
                    {
                        emitter.send(SseEmitter.event()
                            .name("execution-report")
                            .data(Map.of(
                                "clOrdId",   clOrdId,
                                "execType",  String.valueOf(execReportDecoder.execType()),
                                "ordStatus", String.valueOf(execReportDecoder.ordStatus()),
                                "symbol",    execReportDecoder.symbolAsString(),
                                "side",      String.valueOf(execReportDecoder.side()))));
                        emitter.complete();
                    }
                    catch (final Exception e)
                    {
                        emitter.completeWithError(e);
                    }
                    emitterRegistry.remove(clOrdId);
                }
            }
            return Action.CONTINUE;
        }

        @Override
        public void onTimeout(final int libraryId, final Session session)
        {
            System.out.printf("[Client] Session timeout: libraryId=%d sessionId=%d%n",
                libraryId, session.id());
        }

        @Override
        public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
        {
            System.out.printf("[Client] Slow-consumer: hasBecomeSlow=%b sessionId=%d%n",
                hasBecomeSlow, session.id());
        }

        @Override
        public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
        {
            System.out.printf("[Client] Disconnected: sessionId=%d reason=%s%n",
                session.id(), reason);
            return Action.CONTINUE;
        }
    }
}
