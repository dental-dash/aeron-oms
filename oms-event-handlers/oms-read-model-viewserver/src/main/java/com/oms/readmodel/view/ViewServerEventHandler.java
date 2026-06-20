package com.oms.readmodel.view;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.CommandStream;
import com.oms.common.EventHandlerService;
import com.oms.common.annotations.Event;
import com.oms.sbe.*;
import java.nio.file.*;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Projects the sequenced Event Stream (StreamId 2) into an in-memory read model.
 *
 * <p>On startup, replays from the persisted file checkpoint position (0 if none) up to
 * the recorded Archive position, then tracks live image position and periodically writes
 * a binary checkpoint file for fast startup on next restart.
 *
 * <p>Each event is applied in sequence-number order (guaranteed by the Sequencer).
 * The {@code ConcurrentHashMap} allows a future HTTP/WebSocket thread to read
 * {@link #getOrders()} without blocking the AgentRunner thread.
 *
 * M5: OrderEventListener fires onOrderUpdate() after each orders.put().
 * TODO(POC): fsync before rename for production durability
 * TODO(POC): size term buffer based on max replay duration × msg rate
 */
public class ViewServerEventHandler extends EventHandlerService {
    private static final Log log = LogFactory.getLog(ViewServerEventHandler.class);

    private final ConcurrentHashMap<Long, OrderView> orders = new ConcurrentHashMap<>();

    /** Read-only view for query threads (M5 REST/WebSocket layer). */
    public Map<Long, OrderView> getOrders() {
        return Collections.unmodifiableMap(orders);
    }

    public ViewServerEventHandler(CommandStream commands) {
        super(commands);
    }


    @Event
    public void handleOrderAccepted(OrderAcceptedEventDecoder orderAccepted) {
        final OrderView view = new OrderView(
                orderAccepted.orderId(),
                orderAccepted.accountId(),
                orderAccepted.instrument(),
                orderAccepted.side().name(),
                orderAccepted.orderType().name(),
                orderAccepted.timeInForce().name(),
                0L, 0L,
                OrderStatus.OPEN);

        orders.put(view.orderId, view);
        log.info().append("[view-server] ").append(view.toString()).commit();
    }

    @Event
    public void handleOrderFilled(OrderFilledEventDecoder orderFilled) {
        final long orderId = orderFilled.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderFilledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withFill(orderFilled.fillPrice(), orderFilled.fillQuantity());
        orders.put(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    @Event
    public void handleOrderRejected(OrderRejectedEventDecoder orderRejected) {

        // Order never reached OPEN state — no view entry to update
        log.info()
            .append("[view-server] OrderRejectedEvent orderId=").append(orderRejected.orderId())
            .append(" reason=").append(orderRejected.rejectReason().name())
            .commit();
    }

    @Event
    public void handleOrderCancelled(OrderCancelledEventDecoder orderCancelled) {
        final long orderId = orderCancelled.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderCancelledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withCancel();
        orders.put(orderId, updated);
    }

    @Event
    public void handleOrderAmended(OrderAmendedEventDecoder orderAmended) {
        final long orderId = orderAmended.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderAmendedEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withAmend(orderAmended.newPrice(), orderAmended.newQuantity());
        orders.put(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    @Event
    public void handleOrderPartiallyFilled(OrderPartiallyFilledEventDecoder orderPartiallyFilled) {
        final long orderId = orderPartiallyFilled.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderPartiallyFilledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withPartialFill(
                orderPartiallyFilled.fillPrice(),
                orderPartiallyFilled.fillQuantity(),
                orderPartiallyFilled.remainingQty());
        orders.put(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    @Event
    public void handleCancelRejected(CancelRejectedEventDecoder cancelRejected) {
        // Order view unchanged — just log the rejection
        log.info()
            .append("[view-server] CancelRejectedEvent orderId=").append(cancelRejected.orderId())
            .append(" reason=").append(cancelRejected.rejectReason().name())
            .append(" (order unchanged)")
            .commit();
    }
}
