package com.oms.aggregate.client.generated;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.aggregate.client.OrderDomainService;
import com.oms.common.EventStream;
import com.oms.common.OmsStreams;
import com.oms.sbe.*;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;

import java.util.function.Supplier;

public class OrderDomainServiceAgent implements EventStream, Agent {
    private static final Log log = LogFactory.getLog(OrderDomainServiceAgent.class);

    private static final int  AGGREGATE_REPLAY_STREAM_ID = 20;
    private static final long REPLAY_TIMEOUT_MS          = 10_000L;


    // Pre-allocated encoding state — never allocate inside doWork()
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder headerEncoder    = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder    = new MessageHeaderDecoder();

    // Command decoders
    private final NewOrderCommandDecoder     cmdDecoder        = new NewOrderCommandDecoder();
    private final CancelOrderCommandDecoder  cancelCmdDecoder  = new CancelOrderCommandDecoder();
    private final AmendOrderCommandDecoder   amendCmdDecoder   = new AmendOrderCommandDecoder();


    // Event encoders and decoders
    private final NewOrderReceivedEventEncoder  newOrderReceivedEncoder   = new NewOrderReceivedEventEncoder();
    private final NewOrderReceivedEventDecoder  newOrderReceivedDecoder   = new NewOrderReceivedEventDecoder();

    private final OrderAcceptedEventEncoder orderAcceptedEncoder = new OrderAcceptedEventEncoder();
    private final OrderAcceptedEventDecoder orderAcceptedDecoder = new OrderAcceptedEventDecoder();

    private final OrderRejectedEventEncoder  orderRejectedEncoder   = new OrderRejectedEventEncoder();
    private final OrderRejectedEventDecoder  orderRejectedDecoder   = new OrderRejectedEventDecoder();

    private final OrderAmendedEventEncoder orderAmendedEncoder = new OrderAmendedEventEncoder();
    private final OrderAmendedEventDecoder orderAmendedDecoder = new OrderAmendedEventDecoder();

    private final CancelRejectedEventEncoder cancelRejectedEncoder = new CancelRejectedEventEncoder();
    private final CancelRejectedEventDecoder cancelRejectedDecoder = new CancelRejectedEventDecoder();

    private final OrderCancelledEventEncoder orderCancelledEncoder = new OrderCancelledEventEncoder();
    private final OrderCancelledEventDecoder orderCancelledDecoder = new OrderCancelledEventDecoder();

    private final Subscription commandStreamSub;

    private final Publication eventIngressPub;

    private final Aeron        aeron;
    private final AeronArchive archive;

    private OrderDomainService orderService;

    // ── Encoder lookup - cache friendly ───────────────────────────────────────

    private final Class<?>[] classKeys = new Class<?>[6];
    private final Supplier<?>[] encoderFactories = new Supplier<?>[6];

    public OrderDomainServiceAgent(Subscription commandStreamSub, Publication eventIngressPub, Aeron aeron, AeronArchive archive) {
        this.commandStreamSub = commandStreamSub;
        this.eventIngressPub = eventIngressPub;
        this.aeron = aeron;
        this.archive = archive;
        registerEncoders();
    }

    private void registerEncoders() {
        registerEncoder(0, NewOrderReceivedEventEncoder.class, this::newOrderReceivedEncoder);
        registerEncoder(1, OrderAcceptedEventEncoder.class, this::orderAcceptedEncoder);
        registerEncoder(2, OrderRejectedEventEncoder.class,  this::orderRejectedEncoder);
        registerEncoder(3, OrderAmendedEventEncoder.class, this::orderAmendedEncoder);
        registerEncoder(4, CancelRejectedEventEncoder.class, this::cancelRejectedEncoder);
        registerEncoder(5, OrderCancelledEventEncoder.class, this::orderCancelledEncoder);
    }

    // Called dynamically by your annotation processor at startup
    private <T extends MessageEncoderFlyweight> void registerEncoder(int index, Class<T> clazz, Supplier<T> factoryMethod) {
        if (index >= classKeys.length) throw new IllegalStateException("Registry full");
        classKeys[index] = clazz;
        encoderFactories[index] = factoryMethod;
    }

    public void setOrderDomainService(OrderDomainService orderService) {
        this.orderService = orderService;
    }

    @Override
    public int doWork() {
        return commandStreamSub.poll(this::onCommandFragment, 10);
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

        //TODO: Call lifecycle onReplayComplete() or onStarted()
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
        dispatchLocalEvent(headerDecoder.templateId(), buffer, offset);
    }

    // ── Command stream handler ────────────────────────────────────────────────

    private void onCommandFragment(DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch (templateId) {
            case NewOrderCommandDecoder.TEMPLATE_ID:
                cmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                orderService.handleNewOrder(cmdDecoder);
                break;
            case CancelOrderCommandDecoder.TEMPLATE_ID:
                cancelCmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                orderService.handleCancelOrder(cancelCmdDecoder);
                break;
            case AmendOrderCommandDecoder.TEMPLATE_ID:
                amendCmdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                orderService.handleAmendOrder(amendCmdDecoder);
                break;
            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }

    // ── Event encoders ────────────────────────────────────────────────

    private NewOrderReceivedEventEncoder newOrderReceivedEncoder() {
        newOrderReceivedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return newOrderReceivedEncoder;
    }

    private OrderRejectedEventEncoder orderRejectedEncoder() {
        orderRejectedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return orderRejectedEncoder;
    }

    private OrderAcceptedEventEncoder orderAcceptedEncoder() {
        orderAcceptedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return orderAcceptedEncoder;
    }

    private CancelRejectedEventEncoder cancelRejectedEncoder() {
        cancelRejectedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return cancelRejectedEncoder;
    }

    private OrderCancelledEventEncoder orderCancelledEncoder() {
        orderCancelledEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return orderCancelledEncoder;
    }

    private OrderAmendedEventEncoder orderAmendedEncoder() {
        orderAmendedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return orderAmendedEncoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends MessageEncoderFlyweight> T encoderOf(Class<T> encoderClass) {
        // Linear scan of an L1 cache line. Pointer comparison (==) is blazing fast.
        for (int i = 0; i < encoderFactories.length; i++) {
            if (classKeys[i] == encoderClass) {
                // The cast here is safe, and the compiler enforces safety at the call site!
                return (T) encoderFactories[i].get();
            }
        }
        return null;
    }

    @Override
    public void publish(MessageEncoderFlyweight encoder) {
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + encoder.sbeBlockLength();
        // Publish to the Aeron cluster or stream log
        final long result = eventIngressPub.offer(encoder.buffer(), 0, msgLen);

        if (result > 0) {
            // Local Shortcut optimization: Route back to state machine immediately
            dispatchLocalEvent(encoder.sbeTemplateId(), 0);
        } else {
            // TODO: Handle Publication.BACK_PRESSURED or ADMIN_ACTION deterministically
            log.warn()
                    .append("[aggregate] [TODO] handle BACK_PRESSURED and ADMIN_ACTION")
                    .commit();
        }
    }

    /**
     * Unified event router. Dispatches flyweights straight to the user's POJO.
     */
    public void dispatchLocalEvent(int templateId, int offset) {
        switch (templateId) {
            case NewOrderReceivedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                dispatchLocalEvent(templateId, this.newOrderReceivedEncoder.buffer(), offset);
                break;

            case OrderAcceptedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                dispatchLocalEvent(templateId, this.orderAcceptedEncoder.buffer(), offset);
                break;

            case OrderRejectedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 102)
                dispatchLocalEvent(templateId, this.orderRejectedEncoder.buffer(), offset);
                break;

            case OrderCancelledEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 103)
                dispatchLocalEvent(templateId, this.orderCancelledEncoder.buffer(), offset);
                break;

            case OrderAmendedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 103)
                dispatchLocalEvent(templateId, this.orderAmendedEncoder.buffer(), offset);
                break;

            case CancelRejectedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 107)
                dispatchLocalEvent(templateId, this.cancelRejectedEncoder.buffer(), offset);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }

    public void dispatchLocalEvent(int templateId, DirectBuffer buffer, int offset) {
        switch (templateId) {
            case NewOrderReceivedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                this.newOrderReceivedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onNewOrderReceivedEvent(this.newOrderReceivedDecoder);
                break;

            case OrderAcceptedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                this.orderAcceptedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onOrderAcceptedEvent(this.orderAcceptedDecoder);
                break;

            case OrderRejectedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 102)
                this.orderRejectedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onOrderRejectedEvent(this.orderRejectedDecoder);
                break;

            case OrderCancelledEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 103)
                this.orderCancelledDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onOrderCancelledEvent(this.orderCancelledDecoder);
                break;

            case OrderAmendedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 103)
                this.orderAmendedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onOrderAmendedEvent(this.orderAmendedDecoder);
                break;

            case CancelRejectedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 107)
                this.cancelRejectedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.orderService.onCancelRejectedEvent(this.cancelRejectedDecoder);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }
}
