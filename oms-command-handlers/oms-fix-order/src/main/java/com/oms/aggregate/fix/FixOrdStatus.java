package com.oms.aggregate.fix;

/**
 * FIX-level order state machine — mirrors OrdStatus (tag 39) values used in
 * Execution Reports. Managed by {@link FixOrderAggregateAgent} independently of
 * the domain aggregate state.
 */
public enum FixOrdStatus
{
    PENDING_NEW,
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    PENDING_CANCEL,
    CANCELLED,
    REJECTED
}
