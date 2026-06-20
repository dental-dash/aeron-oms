package com.oms.aggregate.client;

import com.oms.common.annotations.CommandApi;
import com.oms.sbe.AmendOrderCommandDecoder;
import com.oms.sbe.CancelOrderCommandDecoder;
import com.oms.sbe.NewOrderCommandDecoder;

@CommandApi
public interface OrderCommandApi {
    void handleNewOrder(NewOrderCommandDecoder newOrder);
    void handleCancelOrder(CancelOrderCommandDecoder cxlOrder);
    void handleAmendOrder(AmendOrderCommandDecoder amendOrder);
}
