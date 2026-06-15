//package com.oms.handlers;
//
//import com.epam.deltix.gflog.api.Log;
//import com.epam.deltix.gflog.api.LogFactory;
//import com.oms.sbe.MessageHeaderDecoder;
//import com.oms.sbe.MessageHeaderEncoder;
//import com.oms.sbe.OrderAcceptedEventDecoder;
//import com.oms.sbe.OrderCancelledEventDecoder;
//import com.oms.sbe.OrderFilledEventEncoder;
//import com.oms.sbe.OrderPartiallyFilledEventEncoder;
//import io.aeron.Publication;
//import io.aeron.Subscription;
//import io.aeron.logbuffer.FragmentHandler;
//import io.aeron.logbuffer.Header;
//import org.agrona.DirectBuffer;
//import org.agrona.concurrent.Agent;
//import org.agrona.concurrent.UnsafeBuffer;
//
//import java.util.HashSet;
//import java.util.LinkedHashMap;
//import java.util.Set;
//
///**
// * Subscribes to the sequenced Event Stream (StreamId 2) and fills every accepted order
// * in two phases: a 50% partial fill followed by a full fill of the remaining quantity.
// *
// * <p>Publishes OrderPartiallyFilledEvent then OrderFilledEvent to Event Ingress (StreamId 11).
// * Cancel awareness: if an OrderCancelledEvent arrives, the pending fill is removed.
// *
// * TODO(POC): replace with a real matching engine in a later milestone.
// */
//public class FillSimulatorHandler implements Agent {
//
//    private static final Log log = LogFactory.getLog(FillSimulatorHandler.class);
//
//    // Pre-allocated — never allocate inside doWork()
//    private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[512]);
//    private final MessageHeaderDecoder headerDecoder         = new MessageHeaderDecoder();
//    private final MessageHeaderEncoder headerEncoder         = new MessageHeaderEncoder();
//    private final OrderAcceptedEventDecoder  acceptedDecoder  = new OrderAcceptedEventDecoder();
//    private final OrderCancelledEventDecoder cancelledDecoder = new OrderCancelledEventDecoder();
//    private final OrderFilledEventEncoder    filledEncoder    = new OrderFilledEventEncoder();
//    private final OrderPartiallyFilledEventEncoder partialEncoder = new OrderPartiallyFilledEventEncoder();
//
//    // LinkedHashMap preserves FIFO insertion order and allows O(1) cancel lookup by orderId.
//    private final LinkedHashMap<Long, PendingFill> pendingFills = new LinkedHashMap<>();
//    private final Set<Long> cancelledOrders = new HashSet<>();
//
//    private final Subscription eventStreamSub;
//    private final Publication  eventIngressPub;
//    private final FragmentHandler fragmentHandler;
//
//    public FillSimulatorHandler(Subscription eventStreamSub, Publication eventIngressPub) {
//        this.eventStreamSub  = eventStreamSub;
//        this.eventIngressPub = eventIngressPub;
//        this.fragmentHandler = this::onFragment;
//    }
//
//    @Override
//    public int doWork() {
//        int work = eventStreamSub.poll(fragmentHandler, 10);
//        work += drainPendingFills();
//        return work;
//    }
//
//    @Override
//    public String roleName() { return "event-handlers"; }
//
//    @Override
//    public void onStart() {
//        log.info().append("event-handlers started").commit();
//    }
//
//    @Override
//    public void onClose() {
//        log.info().append("event-handlers closed").commit();
//    }
//
//    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
//        headerDecoder.wrap(buffer, offset);
//        final int templateId = headerDecoder.templateId();
//
//        if (templateId == OrderAcceptedEventDecoder.TEMPLATE_ID) {
//           // handleOrderAccepted(buffer, offset);
//        } else if (templateId == OrderCancelledEventDecoder.TEMPLATE_ID) {
//           // handleOrderCancelled(buffer, offset);
//        }
//        // Other event types intentionally ignored here
//    }
//
//    private void handleOrderAccepted(DirectBuffer buffer, int offset) {
//        acceptedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
//
//        final long orderId       = acceptedDecoder.orderId();
//        final long accountId     = acceptedDecoder.accountId();
//        final long correlationId = acceptedDecoder.correlationId();
//        final long price         = 0L;//acceptedDecoder.price();
//        final long totalQty      = 0L;//acceptedDecoder.quantity();
//
//        pendingFills.put(orderId, new PendingFill(orderId, accountId, correlationId, price, totalQty, 0L));
//        log.info()
//            .append("[event-handlers] queued partial fill for orderId=").append(orderId)
//            .commit();
//    }
//
//    private void handleOrderCancelled(DirectBuffer buffer, int offset) {
//        cancelledDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
//
//        final long orderId = cancelledDecoder.orderId();
//        pendingFills.remove(orderId);
//        cancelledOrders.add(orderId);
//        log.info()
//            .append("[event-handlers] orderId=").append(orderId)
//            .append(" cancelled — removing from pendingFills")
//            .commit();
//    }
//
//    /**
//     * Process at most 1 pending fill per doWork call to avoid starvation of other agents.
//     * Returns 1 if work was done, 0 if the queue was empty.
//     */
//    private int drainPendingFills() {
//        if (pendingFills.isEmpty()) {
//            return 0;
//        }
//
//        // Peek at first entry without holding an open iterator across map mutations
//        final Long firstKey = pendingFills.keySet().iterator().next();
//        final PendingFill pf = pendingFills.get(firstKey);
//
//        if (pf.filledQty() == 0L) {
//            // Phase 1: publish 50% partial fill
//            final long partialQty   = pf.totalQty() / 2;
//            final long remainingQty = pf.totalQty() - partialQty;
//
//            partialEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
//                    .sequenceNumber(0)   // Sequencer overwrites this
//                    .timestamp(System.nanoTime())
//                    .correlationId(pf.correlationId())
//                    .orderId(pf.orderId())
//                    .accountId(pf.accountId())
//                    .fillPrice(pf.price())
//                    .fillQuantity(partialQty)
//                    .remainingQty(remainingQty);
//
//            final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderPartiallyFilledEventEncoder.BLOCK_LENGTH;
//            final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
//            if (result > 0) {
//                // Advance the pending fill state to reflect what was filled
//                pendingFills.put(pf.orderId(),
//                        new PendingFill(pf.orderId(), pf.accountId(), pf.correlationId(),
//                                        pf.price(), pf.totalQty(), partialQty));
//                log.info()
//                    .append("[event-handlers] published OrderPartiallyFilledEvent orderId=").append(pf.orderId())
//                    .append(" fillQty=").append(partialQty)
//                    .append(" remaining=").append(remainingQty)
//                    .commit();
//            } else {
//                // TODO(POC): add back-pressure retry budget
//                log.warn()
//                    .append("[event-handlers] failed to publish OrderPartiallyFilledEvent orderId=").append(pf.orderId())
//                    .append(" result=").append(result)
//                    .commit();
//            }
//        } else {
//            // Phase 2: publish remaining fill → order fully filled
//            final long remainQty = pf.totalQty() - pf.filledQty();
//
//            filledEncoder.wrapAndApplyHeader(encodingBuffer, 0, headerEncoder)
//                    .sequenceNumber(0)
//                    .timestamp(System.nanoTime())
//                    .correlationId(pf.correlationId())
//                    .orderId(pf.orderId())
//                    .accountId(pf.accountId())
//                    .fillPrice(pf.price())
//                    .fillQuantity(remainQty);
//
//            final int msgLen = MessageHeaderEncoder.ENCODED_LENGTH + OrderFilledEventEncoder.BLOCK_LENGTH;
//            final long result = eventIngressPub.offer(encodingBuffer, 0, msgLen);
//            if (result > 0) {
//                pendingFills.remove(pf.orderId());
//                log.info()
//                    .append("[event-handlers] published OrderFilledEvent orderId=").append(pf.orderId())
//                    .append(" fillQty=").append(remainQty)
//                    .commit();
//            } else {
//                // TODO(POC): add back-pressure retry budget
//                log.warn()
//                    .append("[event-handlers] failed to publish OrderFilledEvent orderId=").append(pf.orderId())
//                    .append(" result=").append(result)
//                    .commit();
//            }
//        }
//
//        return 1;
//    }
//
//    private record PendingFill(
//            long orderId, long accountId, long correlationId,
//            long price, long totalQty, long filledQty) {}
//}
