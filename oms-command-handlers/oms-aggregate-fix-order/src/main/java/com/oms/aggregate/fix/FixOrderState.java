package com.oms.aggregate.fix;

/**
 * FIX-layer per-order state held by {@link FixOrderAggregateAgent}.
 * Keeps the FIX clOrdId→orderId correlation and tracks lifecycle status
 * for building future Execution Reports (M7).
 *
 * <p>Status is mutable because it advances through the FIX state machine
 * ({@link FixOrdStatus}) as domain events arrive back from the aggregate.
 */
public final class FixOrderState
{
    public final String clOrdId;
    public long sessionId;
    public FixOrdStatus status;

    public FixOrderState(final String clOrdId, long sessionId, final FixOrdStatus status)
    {
        this.clOrdId   = clOrdId;
        this.sessionId = sessionId;
        this.status    = status;
    }

    @Override
    public String toString()
    {
        return "FixOrderState{clOrdId='" + clOrdId+ ", sessionId=" + sessionId
            + ", status=" + status + '}';
    }
}
