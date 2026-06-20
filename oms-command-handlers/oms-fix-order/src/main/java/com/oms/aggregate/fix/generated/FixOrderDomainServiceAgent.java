package com.oms.aggregate.fix.generated;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.aggregate.fix.*;
import com.oms.common.EventStream;
import com.oms.fix.sbe.*;
import com.oms.fix.sbe.MessageHeaderDecoder;
import com.oms.fix.sbe.MessageHeaderEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

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
public final class FixOrderDomainServiceAgent implements EventStream, Agent {
    private static final Log log = LogFactory.getLog(FixOrderDomainServiceAgent.class);

    // Enough for header (8) + PlaceOrderCommand block (56) with headroom.
    private static final int ENCODING_BUFFER_SIZE = 512;

    // Pre-allocated output buffer for translated PlaceOrderCommand.
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODING_BUFFER_SIZE));

    // Pre-allocated decoders — reused per fragment, zero allocation on hot path.
    private final MessageHeaderEncoder          headerEncoder = new MessageHeaderEncoder();

    private final MessageHeaderDecoder          headerDecoder = new MessageHeaderDecoder();

    // Command decoders
    private final FixNewOrderSingleCommandDecoder  nosDecoder    = new FixNewOrderSingleCommandDecoder();

    // Event encoders and decoders
    private final FixNewOrderSingleReceivedEventEncoder fixNewOrderSingleReceivedEncoder = new FixNewOrderSingleReceivedEventEncoder();
    private final FixNewOrderSingleReceivedEventDecoder fixNewOrderSingleReceivedDecoder = new FixNewOrderSingleReceivedEventDecoder();

    private final Subscription commandStreamSub;  // IPC stream 1 (sequenced)
    private final Publication eventIngressPub; // IPC stream 10 (back to sequencer)
    private final Aeron        aeron;
    private final AeronArchive archive;

    private FixOrderDomainService fixOrderService;

    // ── Encoder lookup - cache friendly ───────────────────────────────────────

    private final Class<?>[] classKeys = new Class<?>[3];
    private final Supplier<?>[] encoderFactories = new Supplier<?>[3];

    public FixOrderDomainServiceAgent(final Subscription commandStreamSub,
                                      final Publication  eventIngressPub,
                                      Aeron aeron, AeronArchive archive)
    {
        this.commandStreamSub = commandStreamSub;
        this.eventIngressPub = eventIngressPub;
        this.aeron = aeron;
        this.archive = archive;
        registerEncoders();
    }

    private void registerEncoders() {
        registerEncoder(0, FixNewOrderSingleReceivedEventEncoder.class, this::fixNewOrderSingleReceivedEncoder);
    }

    // Called dynamically by your annotation processor at startup
    private <T extends MessageEncoderFlyweight> void registerEncoder(int index, Class<T> clazz, Supplier<T> factoryMethod) {
        if (index >= classKeys.length) throw new IllegalStateException("Registry full");
        classKeys[index] = clazz;
        encoderFactories[index] = factoryMethod;
    }

    public void setFixOrderService(FixOrderDomainService fixOrderService) {
        this.fixOrderService = fixOrderService;
    }

    @Override
    public int doWork()
    {
        return commandStreamSub.poll(this::onCommandFragment, 10);
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

        switch (templateId) {
            case FixNewOrderSingleCommandDecoder.TEMPLATE_ID:
                nosDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                fixOrderService.handleFixNewOrderSingle(nosDecoder);
                break;
            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }

// ── Event encoders ────────────────────────────────────────────────

    private FixNewOrderSingleReceivedEventEncoder fixNewOrderSingleReceivedEncoder() {
        fixNewOrderSingleReceivedEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0);   // Sequencer overwrites this
        return fixNewOrderSingleReceivedEncoder;
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
        final int msgLen = com.oms.sbe.MessageHeaderEncoder.ENCODED_LENGTH + encoder.sbeBlockLength();
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
            case FixNewOrderSingleReceivedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                dispatchLocalEvent(templateId, this.fixNewOrderSingleReceivedEncoder.buffer(), offset);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }

    public void dispatchLocalEvent(int templateId, DirectBuffer buffer, int offset) {
        switch (templateId) {
            case FixNewOrderSingleReceivedEventDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                this.fixNewOrderSingleReceivedDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                this.fixOrderService.onFixNewOrderSingleReceivedEvent(this.fixNewOrderSingleReceivedDecoder);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }
}
