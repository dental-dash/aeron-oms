package com.oms.handlers.generated;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.CommandStream;
import com.oms.common.annotations.EventHandler;
import com.oms.fix.sbe.FixNewOrderSingleReceivedEventDecoder;
import com.oms.sbe.MessageHeaderDecoder;
import com.oms.sbe.MessageHeaderEncoder;
import com.oms.handlers.FixOrderEventHandler;
import com.oms.sbe.*;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;

import java.util.function.Supplier;

/**
 * Subscribes to the sequenced Event Stream (StreamId 2) and fills every accepted order
 * in two phases: a 50% partial fill followed by a full fill of the remaining quantity.
 *
 * <p>Publishes OrderPartiallyFilledEvent then OrderFilledEvent to Event Ingress (StreamId 11).
 * Cancel awareness: if an OrderCancelledEvent arrives, the pending fill is removed.
 *
 * TODO(POC): replace with a real matching engine in a later milestone.
 */
@EventHandler
public class FixOrderEventHandlerAgent implements CommandStream, Agent {

    private static final Log log = LogFactory.getLog(FixOrderEventHandlerAgent.class);

    // Pre-allocated — never allocate inside doWork()
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);

    private final com.oms.fix.sbe.MessageHeaderEncoder   fixHeaderEncoder = new com.oms.fix.sbe.MessageHeaderEncoder();
    private final com.oms.fix.sbe.MessageHeaderDecoder   fixHeaderDecoder = new com.oms.fix.sbe.MessageHeaderDecoder();
    private final MessageHeaderDecoder                 headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder                 headerEncoder = new MessageHeaderEncoder();

    private final FixNewOrderSingleReceivedEventDecoder fixNewOrderSingleReceivedDecoder  = new FixNewOrderSingleReceivedEventDecoder();

    private final NewOrderCommandDecoder newOrderCommandDecoder = new NewOrderCommandDecoder();
    private final NewOrderCommandEncoder newOrderCommandEncoder = new NewOrderCommandEncoder();

    private final Subscription sequencedEventSub;
    private final Publication  ingressCommandPub;
    private final FragmentHandler fragmentHandler;

    private FixOrderEventHandler fixOrderEventHandler;

    private final Class<?>[] classKeys = new Class<?>[6];
    private final Supplier<?>[] encoderFactories = new Supplier<?>[6];

    public FixOrderEventHandlerAgent(Subscription sequencedEventSub, Publication ingressCommandPub) {
        this.sequencedEventSub  = sequencedEventSub;
        this.ingressCommandPub = ingressCommandPub;
        this.fragmentHandler = this::onEventFragment;
        registerEncoders();
    }

    private void registerEncoders() {
        registerEncoder(0, NewOrderCommandEncoder.class, this::newOrderCommandEncoder);
    }

    // Called dynamically by your annotation processor at startup
    private <T extends MessageEncoderFlyweight> void registerEncoder(int index, Class<T> clazz, Supplier<T> factoryMethod) {
        if (index >= classKeys.length) throw new IllegalStateException("Registry full");
        classKeys[index] = clazz;
        encoderFactories[index] = factoryMethod;
    }

    public void setFixOrderEventService(FixOrderEventHandler eventHandler) {
        this.fixOrderEventHandler = eventHandler;
    }


    @Override
    public int doWork() {
        return sequencedEventSub.poll(fragmentHandler, 10);
    }

    @Override
    public String roleName() { return "event-handlers"; }

    @Override
    public void onStart() {
        log.info().append("event-handlers started").commit();
    }

    @Override
    public void onClose() {
        log.info().append("event-handlers closed").commit();
    }

    private void onEventFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch(templateId) {
            case (FixNewOrderSingleReceivedEventDecoder.TEMPLATE_ID):
                fixNewOrderSingleReceivedDecoder.wrapAndApplyHeader(buffer, offset, fixHeaderDecoder);
                this.fixOrderEventHandler.handleFixNewOrderReceived(fixNewOrderSingleReceivedDecoder);
                break;
            default:
                break;
        }
        // Other event types intentionally ignored here
    }

    // ── Command encoders ────────────────────────────────────────────────

    private NewOrderCommandEncoder newOrderCommandEncoder() {
        newOrderCommandEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
                .sequenceNumber(0)   // Sequencer overwrites this
                .timestamp(System.nanoTime());
        return newOrderCommandEncoder;
    }

    @Override
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
        final long result = ingressCommandPub.offer(encoder.buffer(), 0, msgLen);

        if (result > 0) {
            // Local Shortcut optimization: Route back to state machine immediately
            dispatchCommand(encoder.sbeTemplateId(), 0);
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
    public void dispatchCommand(int templateId, int offset) {
        switch (templateId) {
            case NewOrderCommandDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                dispatchCommand(templateId, this.newOrderCommandEncoder.buffer(), offset);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }


    public void dispatchCommand(int templateId, DirectBuffer buffer, int offset) {
        switch (templateId) {
            case NewOrderCommandDecoder.TEMPLATE_ID: // Extracted from @Event(id = 101)
                this.newOrderCommandDecoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
                break;

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }
}
