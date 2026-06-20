package com.oms.aggregate.fix;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.fix.common.Decimal64Util;
import com.oms.fix.sbe.*;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2LongHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * M5: Anti-corruption layer between the FIX acceptor and the core OMS domain.
 *
 * <p>Subscribes to the <em>sequenced</em> Command Stream (IPC stream 1), consumes
 * {@code NewOrderSingleCommand} (templateId=10), performs {@code clOrdId→orderId}
 * mapping, and publishes a {@code PlaceOrderCommand} (templateId=20) back to the
 * Command Ingress Stream (IPC stream 10) so that
 * stamps it and re-publishes it to stream 1 — completing the FIX→domain translation.
 *
 * <p>{@code PlaceOrderCommand} messages arriving on stream 1 (after re-sequencing)
 * are silently skipped — they are our own outbound messages returning.
 *
 * <p>Single-threaded via {@link org.agrona.concurrent.AgentRunner}; all maps and
 * encoders need no synchronisation.
 */
public final class FixOrderAggregateAgent implements FixCommandApi, FixEventApi, Agent {
    private static final Log log = LogFactory.getLog(FixOrderAggregateAgent.class);

    // Enough for header (8) + PlaceOrderCommand block (56) with headroom.
    private static final int ENCODING_BUFFER_SIZE = 512;

    private final Subscription sequencedCommandSub;  // IPC stream 1 (sequenced)
    private final Publication  ingressEventPub; // IPC stream 10 (back to sequencer)

    // clOrdId → internal orderId; missingValue=-1 means "not seen yet"
    private final Object2LongHashMap<String> orderIdByClOrdId =
        new Object2LongHashMap<>(-1L);

    // orderId → FIX-layer state; shared with FixExecReportBridge for ExecReport correlation.
    // ConcurrentHashMap: agent thread writes, bridge reads on the Artio poll thread.
    private final ConcurrentHashMap<Long, FixOrderState> fixOrders;

    private final FixCommandTranslator translator = new FixCommandTranslator();

    // Pre-allocated decoders — reused per fragment, zero allocation on hot path.
    private final MessageHeaderDecoder          headerDecoder = new MessageHeaderDecoder();

    private final MessageHeaderEncoder          headerEncoder = new MessageHeaderEncoder();
    private final FixNewOrderSingleCommandDecoder  nosDecoder    = new FixNewOrderSingleCommandDecoder();

    private final FixNewOrderSingleReceivedEventEncoder fixNosEncoder = new FixNewOrderSingleReceivedEventEncoder();
    private final FixNewOrderSingleReceivedEventDecoder fixNosDecoder = new FixNewOrderSingleReceivedEventDecoder();

    // Pre-allocated output buffer for translated PlaceOrderCommand.
    private final UnsafeBuffer encodingBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODING_BUFFER_SIZE));

    public FixOrderAggregateAgent(final Subscription sequencedCommandSub,
                                  final Publication  ingressEventPub,
                                  final ConcurrentHashMap<Long, FixOrderState> sharedFixStateMap)
    {
        this.sequencedCommandSub = sequencedCommandSub;
        this.ingressEventPub     = ingressEventPub;
        this.fixOrders   = sharedFixStateMap;
    }

    @Override
    public int doWork()
    {
        return sequencedCommandSub.poll(this::onCommandFragment, 10);
    }

    @Override
    public String roleName()
    {
        return "fix-order-aggregate";
    }

    @Override
    public void onStart()
    {
        System.out.println("[fix-order-aggregate] onStart");
    }

    @Override
    public void onClose()
    {
        System.out.println("[fix-order-aggregate] closed");
    }

    // ── Command stream handler ────────────────────────────────────────────────

    private void onCommandFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        if (templateId == FixNewOrderSingleCommandDecoder.TEMPLATE_ID) {
            nosDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            handleFixNewOrderSingle(nosDecoder);
        }
    }

    // ── Command handlers ─────────────────────────────────────────────────────

    @Override
    public void handleFixNewOrderSingle(final FixNewOrderSingleCommandDecoder nos)
    {
        final String clOrdId = nos.clOrdId(); // NUL-padded fixed array → trimmed String

        // Duplicate check — same clOrdId already processed.
        if (orderIdByClOrdId.containsKey(clOrdId))
        {
            System.out.printf("[FixAggAgent] WARN duplicate clOrdId=%s — ignoring%n", clOrdId);
            return;
        }

        // Allocate internal orderId and record both sides of the mapping.
        final long orderId = translator.nextOrderId();

        orderIdByClOrdId.put(clOrdId, orderId);

        System.out.printf("[FixAggAgent] clOrdId=%s → orderId=%d %s%n",
            clOrdId, orderId, FixOrdStatus.PENDING_NEW);

        fixNosEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0L)
                .orderId(orderId)
                .side(nos.side())
                .symbol(nos.symbol())
                .ordType(nos.ordType());

        fixNosEncoder.price().mantissa(nos.price().mantissa()).exponent(nos.price().exponent());
        fixNosEncoder.orderQty().mantissa(nos.orderQty().mantissa()).exponent(nos.orderQty().exponent());

        publishEvent(fixNosEncoder);
    }

    private void publishEvent(FixNewOrderSingleReceivedEventEncoder eventEncoder) {
        final int msgLen = com.oms.fix.sbe.MessageHeaderEncoder.ENCODED_LENGTH + FixNewOrderSingleReceivedEventEncoder.BLOCK_LENGTH;
        final long result = ingressEventPub.offer(eventEncoder.buffer(), 0, msgLen);
        if (result > 0) {
            fixNosDecoder.wrapAndApplyHeader(eventEncoder.buffer(), 0, headerDecoder);
            applyFixNewOrderSingleReceivedEvent(fixNosDecoder);
        } else {
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] failed to publish NewOrderReceivedEvent orderId=").append(fixNosDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }

    @Override
    public void applyFixNewOrderSingleReceivedEvent(FixNewOrderSingleReceivedEventDecoder eventDecoder) {
        final long orderId = eventDecoder.orderId();
        final long quantity = Decimal64Util.fromFixedPoint(eventDecoder.orderQty());
        final long price = Decimal64Util.fromFixedPoint(eventDecoder.price());

        fixOrders.put(orderId, new FixOrderState(eventDecoder.clOrdId(), eventDecoder.sessionId(), FixOrdStatus.PENDING_NEW));

        log.info()
                .append("[fix-order-aggregate] [").append(orderId)
                .append("] New Order Received, symbol=").append(eventDecoder.symbol())
                .append(" qty=").append(quantity)
                .append(" price=").append(price)
                .commit();
    }
}
