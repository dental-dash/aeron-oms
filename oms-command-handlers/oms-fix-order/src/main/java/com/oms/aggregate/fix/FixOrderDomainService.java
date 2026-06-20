package com.oms.aggregate.fix;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.DomainService;
import com.oms.common.EventStream;
import com.oms.common.annotations.CommandHandler;
import com.oms.common.annotations.Event;
import com.oms.fix.common.Decimal64Util;
import com.oms.fix.sbe.FixNewOrderSingleCommandDecoder;
import com.oms.fix.sbe.FixNewOrderSingleReceivedEventDecoder;
import com.oms.fix.sbe.FixNewOrderSingleReceivedEventEncoder;
import org.agrona.collections.Object2LongHashMap;

import java.util.concurrent.ConcurrentHashMap;

@CommandHandler
public class FixOrderDomainService extends DomainService implements FixCommandApi {
    private static final Log log = LogFactory.getLog(FixOrderDomainService.class);

    private final Object2LongHashMap<String> orderIdByClOrdId =
            new Object2LongHashMap<>(-1L);

    private final ConcurrentHashMap<Long, FixOrderState> fixOrders = new ConcurrentHashMap<Long, FixOrderState>();

    private final FixCommandTranslator translator = new FixCommandTranslator();

    public FixOrderDomainService(EventStream events) {
        super(events);
    }

    // ── Command handlers ─────────────────────────────────────────────────────

    @Override
    public void handleFixNewOrderSingle(final FixNewOrderSingleCommandDecoder nos)
    {
        final String clOrdId = nos.clOrdId(); // NUL-padded fixed array → trimmed String

        // Duplicate check — same clOrdId already processed.
        if (orderIdByClOrdId.containsKey(clOrdId))
        {
            System.out.printf("[FixAggAgent] WARN duplicate clOrdId=%s — ignoring%n", clOrdId);
            return;
        }

        // Allocate internal orderId and record both sides of the mapping.
        final long orderId = translator.nextOrderId();

        orderIdByClOrdId.put(clOrdId, orderId);

        System.out.printf("[FixAggAgent] clOrdId=%s → orderId=%d %s%n",
                clOrdId, orderId, FixOrdStatus.PENDING_NEW);

        var fixNewOrderSingleReceivedEvent = events.encoderOf(FixNewOrderSingleReceivedEventEncoder.class)
                .orderId(orderId)
                .side(nos.side())
                .symbol(nos.symbol())
                .ordType(nos.ordType());

        fixNewOrderSingleReceivedEvent.price().mantissa(nos.price().mantissa()).exponent(nos.price().exponent());
        fixNewOrderSingleReceivedEvent.orderQty().mantissa(nos.orderQty().mantissa()).exponent(nos.orderQty().exponent());

        events.publish(fixNewOrderSingleReceivedEvent);
    }

    @Event
    public void onFixNewOrderSingleReceivedEvent(FixNewOrderSingleReceivedEventDecoder eventDecoder) {
        final long orderId = eventDecoder.orderId();
        final long quantity = Decimal64Util.fromFixedPoint(eventDecoder.orderQty());
        final long price = Decimal64Util.fromFixedPoint(eventDecoder.price());

        fixOrders.put(orderId, new FixOrderState(eventDecoder.clOrdId(), eventDecoder.sessionId(), FixOrdStatus.PENDING_NEW));

        log.info()
                .append("[fix-order-aggregate] [").append(orderId)
                .append("] New Order Received, symbol=").append(eventDecoder.symbol())
                .append(" qty=").append(quantity)
                .append(" price=").append(price)
                .commit();
    }
}
