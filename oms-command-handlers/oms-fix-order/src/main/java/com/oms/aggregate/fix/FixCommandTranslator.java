package com.oms.aggregate.fix;

import com.oms.fix.sbe.*;
import com.oms.sbe.MessageHeaderEncoder;
import com.oms.sbe.NewOrderCommandEncoder;
import com.oms.sbe.SideEnum;
import com.oms.sbe.OrdTypeEnum;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Stateful translator: FIX-layer {@code NewOrderSingleCommand} (templateId=10)
 * → domain {@code PlaceOrderCommand} (templateId=20).
 *
 * <p>All encoders are pre-allocated and reused — zero allocation on the hot path.
 * The output is written into the caller-supplied {@link UnsafeBuffer} starting at
 * offset 0; the returned {@code int} is the total encoded byte length to offer.
 *
 * <p>The {@code sequenceNumber} field is set to 0 in every emitted message;
 *  overwrites it when the message
 * re-enters stream 10 and is re-published to stream 1.
 */
public final class FixCommandTranslator
{
    private final MessageHeaderEncoder  headerEncoder = new MessageHeaderEncoder();
    private final NewOrderCommandEncoder cmdEncoder  = new NewOrderCommandEncoder();

    // orderId counter — starts at 1001 to avoid collision with OmsApp's pre-seeded orders.
    // TODO(POC): use a shared atomic counter or coordinate with OmsApp in production.
    private long nextOrderId = 1001L;

    /**
     * Returns the next internal orderId and advances the counter.
     * Call this once per new order; pass the result to {@link #translate}.
     */
    public long nextOrderId()
    {
        return nextOrderId++;
    }

    /**
     * Encodes a {@link NewOrderCommandEncoder} into {@code outBuf} at offset 0.
     *
     * <p>Copies {@code symbol}, {@code side}, {@code ordType}, {@code price},
     * and {@code orderQty} from the decoded NOS. Does NOT copy FIX-specific
     * fields ({@code clOrdId}, {@code sessionId}, {@code account}).
     *
     * @param nos     fully-decoded NOS source (buffer still valid)
     * @param outBuf  pre-allocated output buffer (must be large enough for header + block)
     * @param orderId internal orderId allocated by the caller via {@link #nextOrderId()}
     * @return total encoded length (header + block), suitable for {@code Publication.offer()}
     */
    public int translate(final FixNewOrderSingleCommandDecoder nos,
                         final UnsafeBuffer outBuf,
                         final long orderId)
    {
        cmdEncoder.wrapAndApplyHeader(outBuf, 0, headerEncoder);

        cmdEncoder.sequenceNumber(0L)               // stamped by OmsApp.SequencerAgent
            .orderId(orderId)
            .symbol(nos.symbol())             // String copy — POC acceptable
            .side(nos.side() == com.oms.fix.sbe.SideEnum.BUY ? SideEnum.BUY : SideEnum.SELL)
            .ordType(nos.ordType() == com.oms.fix.sbe.OrdTypeEnum.LIMIT ? OrdTypeEnum.LIMIT : OrdTypeEnum.MARKET);

        // Pass Decimal64 mantissa+exponent through unchanged — no re-scaling needed
        // because FixSessionHandler already encodes price/qty with exponent=-scale().
        cmdEncoder.price()
            .mantissa(nos.price().mantissa())
            .exponent(nos.price().exponent());

        cmdEncoder.orderQty()
            .mantissa(nos.orderQty().mantissa())
            .exponent(nos.orderQty().exponent());

        return MessageHeaderEncoder.ENCODED_LENGTH + NewOrderCommandEncoder.BLOCK_LENGTH;
    }
}
