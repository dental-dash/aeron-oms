package com.oms.handlers;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.CommandStream;
import com.oms.common.EventHandlerService;
import com.oms.common.annotations.Event;
import com.oms.common.annotations.EventHandler;
import com.oms.fix.sbe.FixNewOrderSingleReceivedEventDecoder;
import com.oms.fix.sbe.SideEnum;
import com.oms.sbe.NewOrderCommandEncoder;
import com.oms.sbe.TimeInForceEnum;

/**
 * Subscribes to the sequenced Event Stream (StreamId 2) and fills every accepted order
 * in two phases: a 50% partial fill followed by a full fill of the remaining quantity.
 *
 * <p>Publishes OrderPartiallyFilledEvent then OrderFilledEvent to Event Ingress (StreamId 11).
 * Cancel awareness: if an OrderCancelledEvent arrives, the pending fill is removed.
 *
 * TODO(POC): replace with a real matching engine in a later milestone.
 */
@EventHandler
public class FixOrderEventHandler extends EventHandlerService {
    private static final Log log = LogFactory.getLog(FixOrderEventHandler.class);

    public FixOrderEventHandler(CommandStream commands) {
        super(commands);
    }

    @Event
    public void handleFixNewOrderReceived(FixNewOrderSingleReceivedEventDecoder fixNosReceived) {
        var newOrderCommand = commands.encoderOf(NewOrderCommandEncoder.class)
                .clientId(111)
                .orderId(fixNosReceived.orderId())
                .accountId(222)
                .side(fixNosReceived.side() == SideEnum.BUY ? com.oms.sbe.SideEnum.BUY : com.oms.sbe.SideEnum.SELL)
                .timeInForce(TimeInForceEnum.DAY)
                .text(fixNosReceived.text());

        newOrderCommand.orderQty().mantissa(fixNosReceived.orderQty().mantissa()).exponent(fixNosReceived.orderQty().exponent());
        newOrderCommand.price().mantissa(fixNosReceived.price().mantissa()).exponent(fixNosReceived.price().exponent());

        commands.publish(newOrderCommand);
    }
}
