package com.oms.aggregate.fix;

import com.oms.common.annotations.CommandApi;
import com.oms.fix.sbe.FixNewOrderSingleCommandDecoder;

@CommandApi
public interface FixCommandApi {

    void handleFixNewOrderSingle(FixNewOrderSingleCommandDecoder cmdDecoder);
}
