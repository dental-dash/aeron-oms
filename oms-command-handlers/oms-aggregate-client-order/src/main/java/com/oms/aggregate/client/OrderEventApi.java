package com.oms.aggregate.client;

import com.oms.sbe.*;

public interface OrderEventApi {

    void applyNewOrderReceivedEvent(NewOrderReceivedEventDecoder eventDecoder);
    void applyOrderAcceptedEvent(OrderAcceptedEventDecoder eventDecoder);
    void applyOrderRejectedEvent(OrderRejectedEventDecoder eventDecoder);
    void applyCancelRejectedEvent(CancelRejectedEventDecoder eventDecoder);
    void applyOrderCancelledEvent(OrderCancelledEventDecoder eventDecoder);
    void applyOrderAmmendedEvent(OrderAmendedEventDecoder eventDecoder);
}
