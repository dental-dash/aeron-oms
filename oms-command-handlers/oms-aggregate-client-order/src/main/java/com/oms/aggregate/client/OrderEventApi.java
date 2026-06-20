package com.oms.aggregate.client;

import com.oms.common.EventPublisher;
import com.oms.common.annotations.EventApi;
import com.oms.sbe.*;

@EventApi
public interface OrderEventApi extends EventPublisher {
    NewOrderReceivedEventEncoder newOrderReceivedEncoder();
    OrderRejectedEventEncoder orderRejectedEncoder();
    OrderAcceptedEventEncoder orderAcceptedEncoder();
    CancelRejectedEventEncoder cancelRejectedEncoder();
    OrderCancelledEventEncoder orderCancelledEncoder();
    OrderAmendedEventEncoder orderAmendedEncoder();
}
