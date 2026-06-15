package com.oms.aggregate.client;

import com.oms.sbe.AmendOrderCommandDecoder;
import com.oms.sbe.CancelOrderCommandDecoder;
import com.oms.sbe.NewOrderCommandDecoder;

public interface OrderCommandApi {
    void handleNewOrder(NewOrderCommandDecoder newOrder);
    void handleCancelOrder(CancelOrderCommandDecoder cxlOrder);
    void handleAmendOrder(AmendOrderCommandDecoder amendOrder);
}
