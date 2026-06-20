package com.oms.readmodel.view.generated;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.CommandStream;
import com.oms.common.annotations.EventHandler;
import com.oms.readmodel.view.ViewServerEventHandler;
import com.oms.sbe.*;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
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
public class ViewServerEventHandlerAgent implements CommandStream, Agent {

    private static final Log log = LogFactory.getLog(ViewServerEventHandlerAgent.class);

    // Pre-allocated — never allocate inside doWork()
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);

    private final MessageHeaderDecoder                 headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder                 headerEncoder = new MessageHeaderEncoder();


    private final OrderAcceptedEventDecoder orderAcceptedEventDecoder = new OrderAcceptedEventDecoder();
    private final OrderFilledEventDecoder orderFilledEventDecoder = new OrderFilledEventDecoder();
    private final OrderRejectedEventDecoder orderRejectedEventDecoder = new OrderRejectedEventDecoder();
    private final OrderCancelledEventDecoder orderCancelledEventDecoder = new OrderCancelledEventDecoder();
    private final OrderAmendedEventDecoder orderAmendedEventDecoder = new OrderAmendedEventDecoder();
    private final OrderPartiallyFilledEventDecoder orderPartiallyFilledEventDecoder = new OrderPartiallyFilledEventDecoder();
    private final CancelRejectedEventDecoder cancelRejectedEventDecoder = new CancelRejectedEventDecoder();


    private final Subscription eventStreamSub;
    private final Publication  ingressCommandPub;

    private final Aeron        aeron;
    private final AeronArchive archive;
    private final FragmentHandler fragmentHandler;

    private ViewServerEventHandler viewServerEventHandler;

    private final Class<?>[] classKeys = new Class<?>[6];
    private final Supplier<?>[] encoderFactories = new Supplier<?>[6];

    public ViewServerEventHandlerAgent(Subscription eventStreamSub, Aeron aeron, AeronArchive archive) {
        this.eventStreamSub  = eventStreamSub;
        ingressCommandPub = null;
        this.aeron           = aeron;
        this.archive         = archive;
        this.fragmentHandler = this::onEventFragment;
        registerEncoders();
    }

    private void registerEncoders() {
        // No commands published
    }

    // Called dynamically by your annotation processor at startup
    private <T extends MessageEncoderFlyweight> void registerEncoder(int index, Class<T> clazz, Supplier<T> factoryMethod) {
        if (index >= classKeys.length) throw new IllegalStateException("Registry full");
        classKeys[index] = clazz;
        encoderFactories[index] = factoryMethod;
    }

    public void setViewServerEventHandler(ViewServerEventHandler eventHandler) {
        this.viewServerEventHandler = eventHandler;
    }


    @Override
    public int doWork() {
        return eventStreamSub.poll(fragmentHandler, 10);
    }

    @Override
    public String roleName() { return "view-server"; }

    @Override
    public void onStart() {
        log.info().append("view-sever started").commit();
    }

    @Override
    public void onClose() {
        log.info().append("view-server closed").commit();
    }

    private void onEventFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch(templateId) {
            case (OrderAcceptedEventDecoder.TEMPLATE_ID):
                orderAcceptedEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderAccepted(orderAcceptedEventDecoder);
                break;
            case (OrderFilledEventDecoder.TEMPLATE_ID):
                orderFilledEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderFilled(orderFilledEventDecoder);
                break;
            case (OrderRejectedEventDecoder.TEMPLATE_ID):
                orderRejectedEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderRejected(orderRejectedEventDecoder);
                break;
            case (OrderCancelledEventDecoder.TEMPLATE_ID):
                orderCancelledEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderCancelled(orderCancelledEventDecoder);
                break;
            case (OrderAmendedEventDecoder.TEMPLATE_ID):
                orderAmendedEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderAmended(orderAmendedEventDecoder);
                break;
            case (OrderPartiallyFilledEventDecoder.TEMPLATE_ID):
                orderPartiallyFilledEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleOrderPartiallyFilled(orderPartiallyFilledEventDecoder);
                break;
            case (CancelRejectedEventDecoder.TEMPLATE_ID):
                cancelRejectedEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                this.viewServerEventHandler.handleCancelRejected(cancelRejectedEventDecoder);
                break;
            default:
                break;
        }
        // Other event types intentionally ignored here
    }

    // ── Command encoders ────────────────────────────────────────────────

    // TODO: TBD - how do we know what commands need to be wierd in for the CommandStream
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
        final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + encoder.sbeBlockLength();
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
            // TBD

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }


    public void dispatchCommand(int templateId, DirectBuffer buffer, int offset) {
        switch (templateId) {
            // TBD

            default:
                // Handle unmapped template IDs or telemetry
                break;
        }
    }
}
