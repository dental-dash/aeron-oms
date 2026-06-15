package com.oms.readmodel.view;

/**
 * Immutable snapshot of an order as seen by the view layer.
 *
 * <p>Price and quantity are fixed-point (× 10^8) matching the SBE wire format.
 * Use the {@code with*} factory methods to derive updated copies on each event.
 */
public final class OrderView {

    public final long orderId;
    public final long accountId;
    public final String instrument;
    public final String side;
    public final String orderType;
    public final String tif;
    public final long price;
    public final long quantity;
    public final OrderStatus status;
    public final long lastFillPrice;         // price of the most recent fill leg
    public final long lastFillQty;           // qty of the most recent fill leg
    public final long cumulativeFilledQty;   // running total across all partial fills
    public final long remainingQty;          // quantity - cumulativeFilledQty

    public OrderView(long orderId, long accountId, String instrument,
                     String side, String orderType, String tif,
                     long price, long quantity, OrderStatus status) {
        this(orderId, accountId, instrument, side, orderType, tif,
             price, quantity, status, 0L, 0L, 0L, quantity);
    }

    private OrderView(long orderId, long accountId, String instrument,
                      String side, String orderType, String tif,
                      long price, long quantity, OrderStatus status,
                      long lastFillPrice, long lastFillQty,
                      long cumulativeFilledQty, long remainingQty) {
        this.orderId           = orderId;
        this.accountId         = accountId;
        this.instrument        = instrument;
        this.side              = side;
        this.orderType         = orderType;
        this.tif               = tif;
        this.price             = price;
        this.quantity          = quantity;
        this.status            = status;
        this.lastFillPrice     = lastFillPrice;
        this.lastFillQty       = lastFillQty;
        this.cumulativeFilledQty = cumulativeFilledQty;
        this.remainingQty      = remainingQty;
    }

    /** Returns a new OrderView with status=FILLED and the given fill details. */
    public OrderView withFill(long fillPrice, long fillQty) {
        final long newCumFilled = cumulativeFilledQty + fillQty;
        return new OrderView(orderId, accountId, instrument, side, orderType, tif,
                             price, quantity, OrderStatus.FILLED,
                             fillPrice, fillQty, newCumFilled, 0L);
    }

    /** Returns a new OrderView with status=PARTIALLY_FILLED and updated fill tracking. */
    public OrderView withPartialFill(long fillPrice, long fillQty, long remaining) {
        final long newCumFilled = cumulativeFilledQty + fillQty;
        return new OrderView(orderId, accountId, instrument, side, orderType, tif,
                             price, quantity, OrderStatus.PARTIALLY_FILLED,
                             fillPrice, fillQty, newCumFilled, remaining);
    }

    /** Returns a new OrderView with status=CANCELLED. */
    public OrderView withCancel() {
        return new OrderView(orderId, accountId, instrument, side, orderType, tif,
                             price, quantity, OrderStatus.CANCELLED,
                             lastFillPrice, lastFillQty, cumulativeFilledQty, remainingQty);
    }

    /** Returns a new OrderView with updated price/qty; status remains OPEN or PARTIALLY_FILLED. */
    public OrderView withAmend(long newPrice, long newQty) {
        final long newRemaining = newQty - cumulativeFilledQty;
        return new OrderView(orderId, accountId, instrument, side, orderType, tif,
                             newPrice, newQty, status,
                             lastFillPrice, lastFillQty, cumulativeFilledQty, newRemaining);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(160);
        sb.append("OrderView{orderId=").append(orderId)
          .append(" status=").append(status)
          .append(" price=").append(price / 1e8)
          .append(" qty=").append(quantity / 1e8)
          .append(" cumFilled=").append(cumulativeFilledQty / 1e8)
          .append(" remaining=").append(remainingQty / 1e8);
        if (lastFillPrice > 0) {
            sb.append(" lastFillPrice=").append(lastFillPrice / 1e8)
              .append(" lastFillQty=").append(lastFillQty / 1e8);
        }
        sb.append('}');
        return sb.toString();
    }
}
