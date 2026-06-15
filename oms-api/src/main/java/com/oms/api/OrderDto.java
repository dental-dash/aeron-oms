package com.oms.api;

import com.oms.readmodel.view.OrderView;

/**
 * JSON DTO for a single order. Fixed-point SBE values (price × 10^8) are divided by 1e8.
 * TODO(POC): consider string representation for price/qty to avoid floating-point rounding.
 */
public record OrderDto(
        long orderId, long accountId, String instrument,
        String side, String orderType, String tif,
        double price, double quantity, String status,
        double lastFillPrice, double lastFillQty,
        double cumulativeFilledQty, double remainingQty) {

    private static final double SCALE = 1e8;

    public static OrderDto from(final OrderView v) {
        return new OrderDto(
                v.orderId, v.accountId, v.instrument.trim(),
                v.side, v.orderType, v.tif,
                v.price / SCALE, v.quantity / SCALE, v.status.name(),
                v.lastFillPrice / SCALE, v.lastFillQty / SCALE,
                v.cumulativeFilledQty / SCALE, v.remainingQty / SCALE);
    }
}
