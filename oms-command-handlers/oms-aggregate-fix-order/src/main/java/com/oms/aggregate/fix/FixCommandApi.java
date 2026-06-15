package com.oms.aggregate.fix;

import com.oms.fix.sbe.FixNewOrderSingleCommandDecoder;

public interface FixCommandApi {

    void handleNewOrderSingle(FixNewOrderSingleCommandDecoder cmdDecoder);
}
