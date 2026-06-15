package com.oms.handlers;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.fix.sbe.FixNewOrderSingleReceivedEventDecoder;
import com.oms.fix.sbe.MessageHeaderDecoder;
import com.oms.fix.sbe.MessageHeaderEncoder;
import com.oms.fix.sbe.SideEnum;
import com.oms.sbe.NewOrderCommandDecoder;
import com.oms.sbe.NewOrderCommandEncoder;
import com.oms.sbe.TimeInForceEnum;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Subscribes to the sequenced Event Stream (StreamId 2) and fills every accepted order
 * in two phases: a 50% partial fill followed by a full fill of the remaining quantity.
 *
 * <p>Publishes OrderPartiallyFilledEvent then OrderFilledEvent to Event Ingress (StreamId 11).
 * Cancel awareness: if an OrderCancelledEvent arrives, the pending fill is removed.
 *
 * TODO(POC): replace with a real matching engine in a later milestone.
 */
public class FixOrderEventHandler implements Agent {

    private static final Log log = LogFactory.getLog(FixOrderEventHandler.class);

    // Pre-allocated — never allocate inside doWork()
    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderDecoder                 fixHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder                 fixHeaderEncoder = new MessageHeaderEncoder();
    private final com.oms.sbe.MessageHeaderDecoder     omsHeaderDecoder = new com.oms.sbe.MessageHeaderDecoder();
    private final com.oms.sbe.MessageHeaderEncoder     omsHeaderEncoder = new com.oms.sbe.MessageHeaderEncoder();
    private final FixNewOrderSingleReceivedEventDecoder fixNosReceived  = new FixNewOrderSingleReceivedEventDecoder();

    private final NewOrderCommandDecoder newOrderDecoder = new NewOrderCommandDecoder();
    private final NewOrderCommandEncoder newOrderEncoder = new NewOrderCommandEncoder();

    private final Subscription sequencedEventSub;
    private final Publication  ingressCommandPub;
    private final FragmentHandler fragmentHandler;

    public FixOrderEventHandler(Subscription sequencedEventSub, Publication ingressCommandPub) {
        this.sequencedEventSub  = sequencedEventSub;
        this.ingressCommandPub = ingressCommandPub;
        this.fragmentHandler = this::onEventFragment;
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
        fixHeaderDecoder.wrap(buffer, offset);
        final int templateId = fixHeaderDecoder.templateId();

        if (templateId == FixNewOrderSingleReceivedEventDecoder.TEMPLATE_ID) {
            fixNosReceived.wrapAndApplyHeader(buffer, offset, fixHeaderDecoder);
            handleFixNewOrderReceived(fixNosReceived);
        }
        // Other event types intentionally ignored here
    }

    private void handleFixNewOrderReceived(FixNewOrderSingleReceivedEventDecoder fixNosReceived) {

        newOrderEncoder.wrapAndApplyHeader(encodingBuffer, 0, omsHeaderEncoder)
                .sequenceNumber(0L)
                .clientId(111)
                .orderId(fixNosReceived.orderId())
                .accountId(222)
                .side(fixNosReceived.side() == SideEnum.BUY ? com.oms.sbe.SideEnum.BUY : com.oms.sbe.SideEnum.SELL)
                .timeInForce(TimeInForceEnum.DAY)
                .text(fixNosReceived.text());

        newOrderEncoder.orderQty().mantissa(fixNosReceived.orderQty().mantissa()).exponent(fixNosReceived.orderQty().exponent());
        newOrderEncoder.price().mantissa(fixNosReceived.price().mantissa()).exponent(fixNosReceived.price().exponent());

        publishCommand(newOrderEncoder);
    }

    private void publishCommand(NewOrderCommandEncoder newOrderEncoder) {
        final int msgLen = com.oms.sbe.MessageHeaderEncoder.ENCODED_LENGTH + NewOrderCommandEncoder.BLOCK_LENGTH;
        final long result = ingressCommandPub.offer(newOrderEncoder.buffer(), 0, msgLen);
        if (result <= 0) {
            newOrderDecoder.wrapAndApplyHeader(newOrderEncoder.buffer(), 0, omsHeaderDecoder);
            // TODO(POC): add back-pressure retry budget
            log.warn()
                    .append("[aggregate] [").append(newOrderDecoder.orderId())
                    .append(" result=").append(result)
                    .commit();
        }
    }
}
