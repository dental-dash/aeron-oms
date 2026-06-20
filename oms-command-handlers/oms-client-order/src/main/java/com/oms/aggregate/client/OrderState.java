package com.oms.aggregate.client;

import com.oms.sbe.OrdTypeEnum;
import com.oms.sbe.SideEnum;
import com.oms.sbe.TimeInForceEnum;

/**
 * Mutable in-memory state for a single order, owned by the OrderAggregateAgent thread.
 * Not thread-safe by design — only the aggregate agent thread reads/writes it.
 */
public class OrderState {

    public final long orderId;
    public final long accountId;
    public final String instrument;
    public final SideEnum side;
    public final OrdTypeEnum orderType;
    public final TimeInForceEnum timeInForce;
    public final long price;
    public final long quantity;
    // Mutable — transitions as events are applied
    public OrderStatus status;
    public long currentPrice;       // = price initially; updated on amend
    public long currentQuantity;    // = quantity initially; updated on amend
    public long filledQuantity;     // cumulative fill from observed fill events

    public OrderState(
            long orderId, long accountId, String instrument,
            SideEnum side, OrdTypeEnum orderType, TimeInForceEnum timeInForce,
            long price, long quantity, OrderStatus status) {
        this.orderId         = orderId;
        this.accountId       = accountId;
        this.instrument      = instrument;
        this.side            = side;
        this.orderType       = orderType;
        this.timeInForce     = timeInForce;
        this.price           = price;
        this.quantity        = quantity;
        this.status          = status;
        this.currentPrice    = price;
        this.currentQuantity = quantity;
        this.filledQuantity  = 0L;
    }
}
