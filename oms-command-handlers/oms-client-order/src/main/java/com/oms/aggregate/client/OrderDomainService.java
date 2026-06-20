package com.oms.aggregate.client;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.Decimal64Util;
import com.oms.common.DomainService;
import com.oms.common.EventStream;
import com.oms.common.annotations.CommandHandler;
import com.oms.common.annotations.Event;
import com.oms.sbe.*;

import java.util.HashMap;
import java.util.Map;

@CommandHandler
public class OrderDomainService extends DomainService implements OrderCommandApi {

    private static final Log log = LogFactory.getLog(OrderDomainService.class);

    // In-memory order book — single-threaded, HashMap is fine
    private final Map<Long, OrderState> orders = new HashMap<>();

    public OrderDomainService(EventStream eventStream) {
        super(eventStream);
    }

    // ── Command handlers ──────────────────────────────────────────────────────
    @Override
    public void handleNewOrder(NewOrderCommandDecoder newOrder) {
        final long orderId       = newOrder.orderId();
        final long accountId     = newOrder.accountId();
        final long correlationId = newOrder.correlationId();

        // Accept path — copy all fields from command into the event
        final String instrument   = newOrder.symbol();
        final SideEnum side           = newOrder.side();
        final OrdTypeEnum orderType = newOrder.ordType();
        final TimeInForceEnum tif     = newOrder.timeInForce();

        var newOrderEvent = events.encoderOf(NewOrderReceivedEventEncoder.class)
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .symbol(instrument)
                .side(side)
                .ordType(orderType)
                .timeInForce(tif);
        newOrderEvent.price().mantissa(newOrder.price().mantissa()).exponent(newOrder.price().exponent());
        newOrderEvent.orderQty().mantissa(newOrder.orderQty().mantissa()).exponent(newOrder.orderQty().exponent());

        // Validation — reject path
        RejectReasonEnum rejectReason = null;
        if (newOrder.price().mantissa() <= 0) {
            rejectReason = RejectReasonEnum.INVALID_PRICE;
        }
        else if (newOrder.orderQty().mantissa() <= 0) {
            rejectReason = RejectReasonEnum.INVALID_QUANTITY;
        }
        else if (orders.containsKey(orderId)) {
            rejectReason = RejectReasonEnum.DUPLICATE_ORDER;
        }

        if (rejectReason != null) {
            var orderRejectedEvent = events.encoderOf(OrderRejectedEventEncoder.class)
                    .correlationId(0L)
                    .orderId(orderId)
                    .accountId(0L)
                    .rejectReason(rejectReason);
            events.publish(orderRejectedEvent);
            return;
        }

        events.publish(newOrderEvent);

        var orderAcceptedEvent = events.encoderOf(OrderAcceptedEventEncoder.class)
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .instrument(instrument)
                .side(side)
                .orderType(orderType)
                .timeInForce(tif);
        orderAcceptedEvent.price().mantissa(newOrder.price().mantissa()).exponent(newOrder.price().exponent());
        orderAcceptedEvent.quantity().mantissa(newOrder.orderQty().mantissa()).exponent(newOrder.orderQty().exponent());

        events.publish(orderAcceptedEvent);
    }

    @Override
    public void handleCancelOrder(CancelOrderCommandDecoder cxlOrder) {
        final long orderId       = cxlOrder.orderId();
        final long accountId     = cxlOrder.accountId();
        final long correlationId = cxlOrder.correlationId();
        final CancelReasonEnum reason = cxlOrder.cancelReason();

        final OrderState state = orders.get(orderId);
        if (state == null) {
            log.warn()
                    .append("[aggregate] cancel for unknown orderId=").append(orderId)
                    .commit();
            return;
        }

        // Invalid terminal states — reject the cancel
        if (state.status == OrderStatus.FILLED
                || state.status == OrderStatus.CANCELLED
                || state.status == OrderStatus.REJECTED) {
            // TODO(POC): add ORDER_ALREADY_FILLED / ORDER_ALREADY_CANCELLED reason to RejectReason schema

            var cancelRejectEvent = events.encoderOf(CancelRejectedEventEncoder.class)
                    .correlationId(correlationId)
                    .orderId(orderId)
                    .rejectReason(RejectReasonEnum.RISK_BREACH);

            events.publish(cancelRejectEvent);
            return;
        }

        // OPEN or PARTIALLY_FILLED — allow cancel
        var orderCancelledEvent = events.encoderOf(OrderCancelledEventEncoder.class)
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .cancelReason(reason);

        events.publish(orderCancelledEvent);
    }

    @Override
    public void handleAmendOrder(AmendOrderCommandDecoder amendOrder) {

        final long orderId       = amendOrder.orderId();
        final long accountId     = amendOrder.accountId();
        final long correlationId = amendOrder.correlationId();
        final long newPrice      = amendOrder.newPrice();
        final long newQuantity   = amendOrder.newQuantity();

        final OrderState state = orders.get(orderId);
        if (state == null) {
            log.warn()
                    .append("[aggregate] amend for unknown orderId=").append(orderId)
                    .commit();
            return;
        }

        if (state.status != OrderStatus.OPEN && state.status != OrderStatus.PARTIALLY_FILLED) {
            log.warn()
                    .append("[aggregate] amend rejected orderId=").append(orderId)
                    .append(" status=").append(state.status.name())
                    .commit();
            return;
        }

        if (newPrice <= 0) {
            log.warn()
                    .append("[aggregate] amend rejected orderId=").append(orderId)
                    .append(" invalid newPrice=").append(newPrice)
                    .commit();
            return;
        }

        if (newQuantity <= state.filledQuantity) {
            log.warn()
                    .append("[aggregate] amend rejected orderId=").append(orderId)
                    .append(" newQty=").append(newQuantity)
                    .append(" not above filledQty=").append(state.filledQuantity)
                    .commit();
            return;
        }

        var orderAmendedEvent = events.encoderOf(OrderAmendedEventEncoder.class)
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .newPrice(newPrice)
                .newQuantity(newQuantity);

        events.publish(orderAmendedEvent);
    }

    // ── Event handlers ──────────────────────────────────────────────────────

    @Event
    public void onNewOrderReceivedEvent(NewOrderReceivedEventDecoder eventDecoder) {
        final long orderId = eventDecoder.orderId();
        final long quantity = Decimal64Util.fromFixedPoint(eventDecoder.orderQty());
        final long price = Decimal64Util.fromFixedPoint(eventDecoder.price());
        orders.put(orderId, new OrderState(
                orderId,
                eventDecoder.accountId(),
                eventDecoder.symbol(),
                eventDecoder.side(),
                eventDecoder.ordType(),
                eventDecoder.timeInForce(),
                price,
                quantity,
                OrderStatus.OPEN));

        log.info()
                .append("[order-aggregate] [").append(orderId)
                .append("] New Order Received, symbol=").append(eventDecoder.symbol())
                .append(" qty=").append(quantity)
                .append(" price=").append(price)
                .commit();
    }

    @Event
    public void onOrderAcceptedEvent(OrderAcceptedEventDecoder eventDecoder) {
        final long orderId = eventDecoder.orderId();
        final OrderState state = orders.get(eventDecoder.orderId());
        state.status = OrderStatus.OPEN;

        log.info()
                .append("[order-aggregate] [").append(orderId)
                .append("] Order Accepted, symbol=").append(eventDecoder.instrument())
                .append(" qty=").append(eventDecoder.quantity())
                .append(" price=").append(eventDecoder.price())
                .commit();
    }

    @Event
    public void onOrderRejectedEvent(OrderRejectedEventDecoder eventDecoder) {
        log.info()
                .append("[order-aggregate] [").append(eventDecoder.orderId())
                .append("] Order Rejected, reason=").append(eventDecoder.rejectReason().name())
                .commit();
    }

    @Event
    public void onCancelRejectedEvent(CancelRejectedEventDecoder eventDecoder) {
        log.info()
                .append("[aggregate] [").append(eventDecoder.orderId())
                .append("] Cancel Rejected, reason=").append(eventDecoder.rejectReason().name())
                .commit();
    }

    @Event
    public void onOrderCancelledEvent(OrderCancelledEventDecoder eventDecoder) {
        final OrderState state = orders.get(eventDecoder.orderId());
        state.status = OrderStatus.CANCELLED;
        log.info()
                .append("[aggregate] [").append(eventDecoder.orderId())
                .append("] Order Cancelled")
                .commit();
    }

    @Event
    public void onOrderAmendedEvent(OrderAmendedEventDecoder eventDecoder) {
        final OrderState state = orders.get(eventDecoder.orderId());
        state.currentPrice    = eventDecoder.newPrice();
        state.currentQuantity = eventDecoder.newQuantity();
        log.info()
                .append("[aggregate] [").append(eventDecoder.orderId())
                .append("] newPrice=").append(eventDecoder.newPrice())
                .append(" newQty=").append(eventDecoder.newQuantity())
                .commit();
    }
}
