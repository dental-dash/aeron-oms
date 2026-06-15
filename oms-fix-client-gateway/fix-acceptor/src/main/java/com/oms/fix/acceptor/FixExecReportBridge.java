package com.oms.fix.acceptor;

import com.oms.aggregate.fix.FixOrderState;
import com.oms.sbe.MessageHeaderDecoder;
import com.oms.sbe.OrderAcceptedEventDecoder;
import com.oms.sbe.OrderRejectedEventDecoder;
import com.oms.sbe.SideEnum;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.session.Session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M7: Subscribes to EVENT_STREAM(2) on the OMS Aeron connection and translates
 * {@code OrderAcceptedEvent} / {@code OrderRejectedEvent} into FIX ExecutionReports
 * sent back to the originating FIX session.
 *
 * <p>Must be polled from the same thread as {@code fixLibrary.poll()} — Artio session
 * writes are not thread-safe and must happen on the library polling thread.
 *
 * <p>Events for orderId values that have no entry in {@code fixStateByOrderId} (e.g. orders
 * submitted via the REST API rather than FIX) are silently skipped.
 */
public final class FixExecReportBridge implements FragmentHandler
{
    // OMS SBE decoders — pre-allocated, reused per fragment (zero allocation on hot path).
    private final MessageHeaderDecoder        headerDecoder   = new MessageHeaderDecoder();
    private final OrderAcceptedEventDecoder   acceptedDecoder = new OrderAcceptedEventDecoder();
    private final OrderRejectedEventDecoder   rejectedDecoder = new OrderRejectedEventDecoder();

    // Artio encoder and timestamp encoder — pre-allocated.
    private final ExecutionReportEncoder execReport = new ExecutionReportEncoder();
    private final UtcTimestampEncoder    tsEncoder  = new UtcTimestampEncoder();

    // Monotonically-increasing ExecID sequence — unique per process restart (POC acceptable).
    // TODO(POC): persist across restarts for production.
    private final AtomicLong execIdSeq = new AtomicLong(1);

    // Shared with FixOrderAggregateAgent (written on agent thread, read here on Artio poll thread).
    private final ConcurrentHashMap<Long, FixOrderState> fixStateByOrderId;

    // Shared with FixSessionHandler (written/removed on Artio poll thread — same thread as reads here).
    private final ConcurrentHashMap<Long, Session> activeSessions;

    public FixExecReportBridge(final ConcurrentHashMap<Long, FixOrderState> fixStateByOrderId,
                               final ConcurrentHashMap<Long, Session> activeSessions)
    {
        this.fixStateByOrderId = fixStateByOrderId;
        this.activeSessions    = activeSessions;
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset,
                           final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        if (templateId == OrderAcceptedEventDecoder.TEMPLATE_ID)
        {
            acceptedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            onOrderAccepted(acceptedDecoder);
        }
        else if (templateId == OrderRejectedEventDecoder.TEMPLATE_ID)
        {
            rejectedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            onOrderRejected(rejectedDecoder);
        }
        // Other event types (fills, cancels) not handled in M7 — skip silently.
    }

    // ── Event handlers ───────────────────────────────────────────────────────

    private void onOrderAccepted(final OrderAcceptedEventDecoder event)
    {
        final long orderId = event.orderId();
        final FixOrderState state = fixStateByOrderId.get(orderId);
        if (state == null)
        {
            // Not a FIX-originated order (e.g. REST API) — skip.
            return;
        }

        final Session session = activeSessions.get(state.sessionId);
        if (session == null)
        {
            System.err.printf("[ExecReportBridge] WARN session not found: sessionId=%d orderId=%d%n",
                state.sessionId, orderId);
            return;
        }

        // Map OMS Side enum → FIX 4.4 side char ('1'=BUY, '2'=SELL).
        final char fixSide = event.side() == SideEnum.BUY ? '1' : '2';

        execReport.reset();
        // TODO(POC): use exchange-assigned orderID in production; for now reflect clOrdId so the client can correlate via SSE.
        execReport.orderID(state.clOrdId);
        execReport.execID(Long.toString(execIdSeq.getAndIncrement()));
        execReport.execType('0');      // 0 = New
        execReport.ordStatus('0');     // 0 = New
        execReport.instrument().symbol(event.instrument().trim());
        execReport.side(fixSide);

        final int tsLen = tsEncoder.encode(System.currentTimeMillis());
        execReport.transactTime(tsEncoder.buffer(), 0, tsLen);

        final long result = session.trySend(execReport);
        if (result < 0)
        {
            // TODO(POC): back-pressure handling — log and drop in POC; retry queue in production.
            System.err.printf("[ExecReportBridge] WARN trySend back-pressure: result=%d orderId=%d%n",
                result, orderId);
        }
        else
        {
            System.out.printf("[ExecReportBridge] Sent ER orderId=%d clOrdId=%s execType=0 (NEW)%n",
                orderId, state.clOrdId);
        }
    }

    private void onOrderRejected(final OrderRejectedEventDecoder event)
    {
        final long orderId = event.orderId();
        final FixOrderState state = fixStateByOrderId.get(orderId);
        if (state == null)
        {
            return;
        }

        final Session session = activeSessions.get(state.sessionId);
        if (session == null)
        {
            System.err.printf("[ExecReportBridge] WARN session not found: sessionId=%d orderId=%d%n",
                state.sessionId, orderId);
            return;
        }

        execReport.reset();
        // TODO(POC): use exchange-assigned orderID in production; for now reflect clOrdId so the client can correlate via SSE.
        execReport.orderID(state.clOrdId);
        execReport.execID(Long.toString(execIdSeq.getAndIncrement()));
        execReport.execType('8');      // 8 = Rejected
        execReport.ordStatus('8');     // 8 = Rejected
        // symbol and side not available in OrderRejectedEvent — use placeholder.
        // TODO(POC): store symbol/side in FixOrderState so rejected ER is fully populated.
        execReport.instrument().symbol("N/A");
        execReport.side('1');          // placeholder — required FIX field

        final int tsLen = tsEncoder.encode(System.currentTimeMillis());
        execReport.transactTime(tsEncoder.buffer(), 0, tsLen);

        final long result = session.trySend(execReport);
        if (result < 0)
        {
            System.err.printf("[ExecReportBridge] WARN trySend back-pressure: result=%d orderId=%d%n",
                result, orderId);
        }
        else
        {
            System.out.printf("[ExecReportBridge] Sent ER orderId=%d clOrdId=%s execType=8 (REJECTED) reason=%s%n",
                orderId, state.clOrdId, event.rejectReason());
        }
    }
}
