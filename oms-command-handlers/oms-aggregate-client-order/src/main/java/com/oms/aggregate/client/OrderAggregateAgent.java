package com.oms.aggregate.client;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.Decimal64Util;
import com.oms.sbe.*;
import com.oms.common.OmsStreams;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Subscribes to the sequenced Command Stream (StreamId 1) and Event Stream (StreamId 2).
 * Validates commands, maintains order state, and publishes events to Event Ingress (StreamId 11).
 *
 * <p>On startup, replays the full Event Stream from Archive to rebuild the in-memory
 * {@code orders} map before accepting any live commands. This ensures correct duplicate
 * detection and state-machine transitions after a process restart.
 *
 * <p>Observes fill events from the live Event Stream to guard cancel/amend on FILLED orders.
 * All state is single-threaded — only this agent's AgentRunner thread touches {@code orders}.
 *
 * TODO(POC): Archive replay blocks onStart() — live commands buffer in Aeron term buffer.
 *            Size term buffer based on max replay duration × msg rate.
 */
public class OrderAggregateAgent implements OrderCommandApi, OrderEventApi, Agent {
    private static final Log log = LogFactory.getLog(OrderAggregateAgent.class);

    private static final int  AGGREGATE_REPLAY_STREAM_ID = 20;
    private static final long REPLAY_TIMEOUT_MS          = 10_000L;

    // Pre-allocated encoding state — never allocate inside doWork()
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderDecoder headerDecoder    = new MessageHeaderDecoder();
    // Separate fix-sbe header decoder for PlaceOrderCommand cross-schema wrapAndApplyHeader.
    // Both schemas use identical 8-byte header wire format; the type must match the encoder used.
    private final com.oms.fix.sbe.MessageHeaderDecoder fixHeaderDecoder =
        new com.oms.fix.sbe.MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder    = new MessageHeaderEncoder();
    private final NewOrderCommandDecoder     cmdDecoder        = new NewOrderCommandDecoder();
    private final CancelOrderCommandDecoder  cancelCmdDecoder  = new CancelOrderCommandDecoder();
    private final AmendOrderCommandDecoder   amendCmdDecoder   = new AmendOrderCommandDecoder();
    private final OrderFilledEventDecoder    filledDecoder     = new OrderFilledEventDecoder();
    private final OrderPartiallyFilledEventDecoder partialDecoder = new OrderPartiallyFilledEventDecoder();
    private final NewOrderReceivedEventEncoder  newOrderReceivedEncoder   = new NewOrderReceivedEventEncoder();
    private final NewOrderReceivedEventDecoder  newOrderReceivedDecoder   = new NewOrderReceivedEventDecoder();
    private final OrderAcceptedEventEncoder  acceptedEncoder   = new OrderAcceptedEventEncoder();
    private final OrderRejectedEventEncoder  rejectedEncoder   = new OrderRejectedEventEncoder();
    private final OrderRejectedEventDecoder rejectedDecoder   = new OrderRejectedEventDecoder();
    private final OrderCancelledEventEncoder orderCancelledEncoder = new OrderCancelledEventEncoder();
    private final OrderCancelledEventDecoder orderCancelledDecoder = new OrderCancelledEventDecoder();
    private final OrderAmendedEventEncoder orderAmendedEncoder = new OrderAmendedEventEncoder();
    private final OrderAmendedEventDecoder orderAmendedDecoder = new OrderAmendedEventDecoder();
    private final CancelRejectedEventEncoder cancelRejectedEncoder = new CancelRejectedEventEncoder();
    private final CancelRejectedEventDecoder cancelRejectedDecoder = new CancelRejectedEventDecoder();

    // Pre-allocated replay decoders — reused across all replayed messages
    private final OrderAcceptedEventDecoder  replayAcceptedDecoder  = new OrderAcceptedEventDecoder();
    private final OrderCancelledEventDecoder replayCancelledDecoder = new OrderCancelledEventDecoder();


    private final Subscription commandStreamSub;
    private final Subscription eventStreamSub;
    private final Publication  eventIngressPub;
    private final FragmentHandler commandHandler;
//    private final FragmentHandler eventHandler;
    private final Aeron        aeron;
    private final AeronArchive archive;

    // In-memory order book — single-threaded, HashMap is fine
    private final Map<Long, OrderState> orders = new HashMap<>();

    public OrderAggregateAgent(Subscription commandStreamSub, Publication eventIngressPub,
                               Subscription eventStreamSub, Aeron aeron, AeronArchive archive) {
        this.commandStreamSub = commandStreamSub;
        this.eventIngressPub  = eventIngressPub;
        this.eventStreamSub   = eventStreamSub;
        this.aeron            = aeron;
        this.archive          = archive;
        // Allocate handler references once — avoid lambda re-allocation on hot path
        this.commandHandler   = this::onCommandFragment;
//        this.eventHandler     = this::onEventFragment;
    }

    @Override
    public int doWork() {
        return commandStreamSub.poll(commandHandler, 10);
//        work += eventStreamSub.poll(eventHandler, 10);
    }

    @Override
    public String roleName() { return "order-aggregate"; }

    /**
     * Replays the full Event Stream from Archive position 0 to the recorded stop/live position.
     * Blocks until replay image closes (i.e., Archive has delivered all requested bytes).
     * Live commands are safely buffered in the Aeron term buffer during replay.
     */
    @Override
    public void onStart() {
        log.info().append("[aggregate] starting — replaying Event Stream from Archive...").commit();

        final long[] foundRecordingId  = {-1L};
        final long[] foundStopPosition = {AeronArchive.NULL_POSITION};

        final int found = archive.listRecordingsForUri(
                0, 1, OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    foundRecordingId[0]  = recordingId;
                    foundStopPosition[0] = stopPosition;  // NULL_POSITION if still active
                });

        if (found == 0 || foundRecordingId[0] < 0) {
            log.info().append("[aggregate] no recording found — starting fresh").commit();
            return;
        }

        final long recordingId = foundRecordingId[0];

        // For a stopped recording (restart case), stopPosition has the persisted end position.
        // For an active recording (same boot), fall back to the live position.
        long currentPos = foundStopPosition[0];
        if (currentPos == AeronArchive.NULL_POSITION) {
            currentPos = archive.getRecordingPosition(recordingId);
        }

        if (currentPos <= 0) {
            log.info().append("[aggregate] recording empty — starting fresh").commit();
            return;
        }

        log.info().append("[aggregate] replaying recordingId=").append(recordingId)
            .append(" length=").append(currentPos).commit();

        final long replaySessionId = archive.startReplay(
                recordingId, 0L, currentPos, OmsStreams.IPC, AGGREGATE_REPLAY_STREAM_ID);

        try (final Subscription replaySub = aeron.addSubscription(
                     OmsStreams.IPC, AGGREGATE_REPLAY_STREAM_ID)) {

            final long deadlineMs = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            Image replayImage = null;
            while (replayImage == null) {
                replayImage = replaySub.imageBySessionId((int) replaySessionId);
                if (System.currentTimeMillis() > deadlineMs) {
                    throw new IllegalStateException("[aggregate] replay image connect timeout");
                }
                Thread.yield();
            }

            // Poll until the image closes — Archive closes it after delivering currentPos bytes.
            while (!replayImage.isClosed()) {
                final int frags = replayImage.poll(this::applyReplayEvent, 256);
                if (frags == 0) Thread.yield();
            }
        }

        log.info().append("[aggregate] replay complete — orders rebuilt=").append(orders.size()).commit();
    }

    @Override
    public void onClose() {
        log.info().append("order-aggregate closed").commit();
    }

    // ── Replay handler ────────────────────────────────────────────────────────

    /**
     * Applies archived Event Stream messages to reconstruct the in-memory {@code orders} map.
     * Mirrors the live event observer but without any publishing or logging.
     * Called only during {@link #onStart()} replay — never on the live hot path.
     */
    private void applyReplayEvent(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        switch (headerDecoder.templateId()) {
            case OrderAcceptedEventDecoder.TEMPLATE_ID -> {
                replayAcceptedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                applyOrderAcceptedEvent(replayAcceptedDecoder);
            }
            case OrderRejectedEventDecoder.TEMPLATE_ID -> {
                rejectedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                applyOrderRejectedEvent(rejectedDecoder);
            }
            case CancelRejectedEventDecoder.TEMPLATE_ID -> {
                cancelRejectedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                applyCancelRejectedEvent(cancelRejectedDecoder);
            }
            case OrderCancelledEventDecoder.TEMPLATE_ID -> {
                replayCancelledDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                applyOrderCancelledEvent(replayCancelledDecoder);
            }
            case OrderAmendedEventDecoder.TEMPLATE_ID -> {
                orderAmendedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                applyOrderAmmendedEvent(orderAmendedDecoder);
            }
        }
    }

    // ── Command stream handler ────────────────────────────────────────────────

    private void onCommandFragment(DirectBuffer buffer, final int offset, final int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        if (templateId == NewOrderCommandDecoder.TEMPLATE_ID) {
            cmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            handleNewOrder(cmdDecoder);
        } else if (templateId == CancelOrderCommandDecoder.TEMPLATE_ID) {
            cancelCmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            handleCancelOrder(cancelCmdDecoder);
        } else if (templateId == AmendOrderCommandDecoder.TEMPLATE_ID) {
            amendCmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            handleAmendOrder(amendCmdDecoder);
        }
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

        newOrderReceivedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime())
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .symbol(instrument)
                .side(side)
                .ordType(orderType)
                .timeInForce(tif);
        newOrderReceivedEncoder.price().mantissa(newOrder.price().mantissa()).exponent(newOrder.price().exponent());
        newOrderReceivedEncoder.orderQty().mantissa(newOrder.orderQty().mantissa()).exponent(newOrder.orderQty().exponent());

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
            publishEvent(newOrderReceivedEncoder);

            rejectedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                    .sequenceNumber(0)
                    .timestamp(System.nanoTime())
                    .correlationId(0L)
                    .orderId(orderId)
                    .accountId(0L)
                    .rejectReason(rejectReason);
            publishEvent(rejectedEncoder);
            return;
        }

        publishEvent(newOrderReceivedEncoder);

        acceptedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime())
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .instrument(instrument)
                .side(side)
                .orderType(orderType)
                .timeInForce(tif);
        acceptedEncoder.price().mantissa(newOrder.price().mantissa()).exponent(newOrder.price().exponent());
        acceptedEncoder.quantity().mantissa(newOrder.orderQty().mantissa()).exponent(newOrder.orderQty().exponent());

        publishEvent(acceptedEncoder);
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
            cancelRejectedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                    .sequenceNumber(0)
                    .timestamp(System.nanoTime())
                    .correlationId(correlationId)
                    .orderId(orderId)
                    .rejectReason(RejectReasonEnum.RISK_BREACH);

            publishEvent(cancelRejectedEncoder);
            return;
        }

        // OPEN or PARTIALLY_FILLED — allow cancel
        orderCancelledEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)
                .timestamp(System.nanoTime())
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .cancelReason(reason);

        publishEvent(orderCancelledEncoder);
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

        orderAmendedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)
                .timestamp(System.nanoTime())
                .correlationId(correlationId)
                .orderId(orderId)
                .accountId(accountId)
                .newPrice(newPrice)
                .newQuantity(newQuantity);

        publishEvent(orderAmendedEncoder);
    }

    // ── Event publishers ──────────────────────────────────────────────────────

    private void publishEvent(NewOrderReceivedEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + NewOrderReceivedEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(eventEncoder.buffer(), 0, msgLen);
        if (result > 0) {
            newOrderReceivedDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyNewOrderReceivedEvent(newOrderReceivedDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] failed to publish NewOrderReceivedEvent orderId=").append(newOrderReceivedDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }

    private void publishEvent(OrderAcceptedEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderAcceptedEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(eventEncoder.buffer(), 0, msgLen);
        if (result > 0) {
            replayAcceptedDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyOrderAcceptedEvent(replayAcceptedDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] failed to publish OrderAcceptedEvent orderId=").append(replayAcceptedDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }

    private void publishEvent(OrderRejectedEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderRejectedEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
        if (result > 0) {
            rejectedDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyOrderRejectedEvent(rejectedDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] failed to publish OrderRejectedEvent orderId=").append(rejectedDecoder.orderId())
                    .commit();
        }
    }

    private void publishEvent(CancelRejectedEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + CancelRejectedEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
        if (result > 0) {
            cancelRejectedDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyCancelRejectedEvent(cancelRejectedDecoder);
        } else {
            log.warn()
                    .append("[aggregate] failed to publish CancelRejectedEvent orderId=").append(cancelRejectedDecoder.orderId())
                    .commit();
        }
    }

    private void publishEvent(OrderCancelledEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderCancelledEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
        if (result > 0) {
            orderCancelledDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyOrderCancelledEvent(orderCancelledDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] failed to publish OrderCancelledEvent orderId=").append(orderCancelledDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }

    private void publishEvent(OrderAmendedEventEncoder eventEncoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderAmendedEventEncoder.BLOCK_LENGTH;
        final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
        if (result > 0) {
            orderAmendedDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] [").append(orderAmendedDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }

    // ── Event handlers ──────────────────────────────────────────────────────

    @Override
    public void applyNewOrderReceivedEvent(NewOrderReceivedEventDecoder eventDecoder) {
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
                .append("] New Order Received, symbol=").append(cmdDecoder.symbol())
                .append(" qty=").append(quantity)
                .append(" price=").append(price)
                .commit();
    }

    @Override
    public void applyOrderAcceptedEvent(OrderAcceptedEventDecoder eventDecoder) {
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

    @Override
    public void applyOrderRejectedEvent(OrderRejectedEventDecoder eventDecoder) {
        log.info()
                .append("[order-aggregate] [").append(eventDecoder.orderId())
                .append("] Order Rejected, reason=").append(eventDecoder.rejectReason().name())
                .commit();
    }

    @Override
    public void applyCancelRejectedEvent(CancelRejectedEventDecoder eventDecoder) {
        log.info()
                .append("[aggregate] [").append(eventDecoder.orderId())
                .append("] Cancel Rejected, reason=").append(eventDecoder.rejectReason().name())
                .commit();
    }

    @Override
    public void applyOrderCancelledEvent(OrderCancelledEventDecoder eventDecoder) {
        final OrderState state = orders.get(eventDecoder.orderId());
        state.status = OrderStatus.CANCELLED;
        log.info()
                .append("[aggregate] [").append(eventDecoder.orderId())
                .append("] Order Cancelled")
                .commit();
    }

    @Override
    public void applyOrderAmmendedEvent(OrderAmendedEventDecoder eventDecoder) {
        final OrderState state = orders.get(orderAmendedDecoder.orderId());
        state.currentPrice    = orderAmendedDecoder.newPrice();
        state.currentQuantity = orderAmendedDecoder.newQuantity();
        log.info()
                .append("[aggregate] [").append(orderAmendedDecoder.orderId())
                .append("] newPrice=").append(orderAmendedDecoder.newPrice())
                .append(" newQty=").append(orderAmendedDecoder.newQuantity())
                .commit();
    }
}
