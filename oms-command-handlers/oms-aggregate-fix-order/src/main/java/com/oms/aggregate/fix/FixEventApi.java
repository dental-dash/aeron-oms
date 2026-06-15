package com.oms.aggregate.fix;

import com.oms.fix.sbe.FixNewOrderSingleReceivedEventDecoder;

public interface FixEventApi {

    void applyFixNewOrderSingleReceivedEvent(FixNewOrderSingleReceivedEventDecoder eventDecoder);
}
