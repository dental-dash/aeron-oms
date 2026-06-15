// Placed in uk.co.real_logic.artio.builder to gain package-private access to
// TrailerEncoder.startTrailer / finishMessage and HeaderEncoder.finishHeader.
// TODO(POC): replace with Artio codegen-generated encoder from a full FIX 4.4 dictionary.
package uk.co.real_logic.artio.builder;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.EncodingException;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.ReadOnlyDecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.*;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.builder.Validation.CODEC_VALIDATION_ENABLED;

/**
 * Hand-rolled FIX 4.4 OrderCancelReplaceRequest (35=G) encoder, following the same
 * generated pattern as {@code NewOrderSingleEncoder} in artio-session-codecs.
 *
 * <p>Fields: ClOrdID(11), OrigClOrdID(41), Symbol(55), Side(54), OrdType(40),
 * Price(44), OrderQty(38), TransactTime(60).
 *
 * // TODO(POC): replace with Artio codegen-generated encoder from a full FIX 4.4 dictionary.
 */
public class OrderCancelReplaceRequestEncoder implements Encoder
{
    // FIX message type 'G' = 71
    public long messageType() { return 71L; }

    public OrderCancelReplaceRequestEncoder()
    {
        header.msgType("G");
    }

    private final TrailerEncoder trailer = new TrailerEncoder();
    public TrailerEncoder trailer() { return trailer; }

    private final HeaderEncoder header = new HeaderEncoder();
    public SessionHeaderEncoder header() { return header; }

    // tag 11 = "11="
    private static final int    clOrdIDHeaderLength = 3;
    private static final byte[] clOrdIDHeader       = new byte[]{49, 49, (byte) '='};

    // tag 41 = "41="
    private static final int    origClOrdIDHeaderLength = 3;
    private static final byte[] origClOrdIDHeader       = new byte[]{52, 49, (byte) '='};

    // tag 54 = "54="
    private static final int    sideHeaderLength = 3;
    private static final byte[] sideHeader       = new byte[]{53, 52, (byte) '='};

    // tag 40 = "40="
    private static final int    ordTypeHeaderLength = 3;
    private static final byte[] ordTypeHeader       = new byte[]{52, 48, (byte) '='};

    // tag 44 = "44="
    private static final int    priceHeaderLength = 3;
    private static final byte[] priceHeader       = new byte[]{52, 52, (byte) '='};

    // tag 60 = "60="
    private static final int    transactTimeHeaderLength = 3;
    private static final byte[] transactTimeHeader       = new byte[]{54, 48, (byte) '='};

    private final MutableDirectBuffer clOrdID        = new UnsafeBuffer();
    private byte[] clOrdIDInternalBuffer              = clOrdID.byteArray();
    private int    clOrdIDOffset                      = 0;
    private int    clOrdIDLength                      = 0;

    private final MutableDirectBuffer origClOrdID     = new UnsafeBuffer();
    private byte[] origClOrdIDInternalBuffer           = origClOrdID.byteArray();
    private int    origClOrdIDOffset                   = 0;
    private int    origClOrdIDLength                   = 0;

    private final InstrumentEncoder instrument = new InstrumentEncoder();
    public InstrumentEncoder instrument() { return instrument; }

    private char    side    = MISSING_CHAR;
    private boolean hasSide = false;

    private char    ordType    = MISSING_CHAR;
    private boolean hasOrdType = false;

    private final DecimalFloat price    = new DecimalFloat();
    private boolean            hasPrice = false;

    private final OrderQtyDataEncoder orderQtyData = new OrderQtyDataEncoder();
    public OrderQtyDataEncoder orderQtyData() { return orderQtyData; }

    private final MutableDirectBuffer transactTime = new UnsafeBuffer();
    private byte[] transactTimeInternalBuffer        = transactTime.byteArray();
    private int    transactTimeOffset                = 0;
    private int    transactTimeLength                = 0;

    // ── Setters ──────────────────────────────────────────────────────────────

    public OrderCancelReplaceRequestEncoder clOrdID(final CharSequence value)
    {
        if (toBytes(value, clOrdID)) { clOrdIDInternalBuffer = clOrdID.byteArray(); }
        clOrdIDOffset = 0;
        clOrdIDLength = value.length();
        return this;
    }

    public OrderCancelReplaceRequestEncoder origClOrdID(final CharSequence value)
    {
        if (toBytes(value, origClOrdID)) { origClOrdIDInternalBuffer = origClOrdID.byteArray(); }
        origClOrdIDOffset = 0;
        origClOrdIDLength = value.length();
        return this;
    }

    public OrderCancelReplaceRequestEncoder side(final char value)
    {
        side    = value;
        hasSide = true;
        return this;
    }

    public OrderCancelReplaceRequestEncoder ordType(final char value)
    {
        ordType    = value;
        hasOrdType = true;
        return this;
    }

    public OrderCancelReplaceRequestEncoder price(final long value, final int scale)
    {
        price.set(value, scale);
        hasPrice = true;
        return this;
    }

    public OrderCancelReplaceRequestEncoder price(final ReadOnlyDecimalFloat value)
    {
        price.set(value);
        hasPrice = true;
        return this;
    }

    public OrderCancelReplaceRequestEncoder transactTime(final byte[] value, final int offset, final int length)
    {
        transactTime.wrap(value);
        transactTimeOffset = offset;
        transactTimeLength = length;
        return this;
    }

    // ── Encoder ──────────────────────────────────────────────────────────────

    @Override
    public long encode(final MutableAsciiBuffer buffer, final int offset)
    {
        final long startMessageResult = header.startMessage(buffer, offset);
        final int bodyStart = Encoder.offset(startMessageResult);
        int position = bodyStart + Encoder.length(startMessageResult);

        if (clOrdIDLength > 0)
        {
            buffer.putBytes(position, clOrdIDHeader, 0, clOrdIDHeaderLength);
            position += clOrdIDHeaderLength;
            buffer.putBytes(position, clOrdID, clOrdIDOffset, clOrdIDLength);
            position += clOrdIDLength;
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: ClOrdID");
        }

        if (origClOrdIDLength > 0)
        {
            buffer.putBytes(position, origClOrdIDHeader, 0, origClOrdIDHeaderLength);
            position += origClOrdIDHeaderLength;
            buffer.putBytes(position, origClOrdID, origClOrdIDOffset, origClOrdIDLength);
            position += origClOrdIDLength;
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: OrigClOrdID");
        }

        position += instrument.encode(buffer, position);

        if (hasSide)
        {
            buffer.putBytes(position, sideHeader, 0, sideHeaderLength);
            position += sideHeaderLength;
            position += buffer.putCharAscii(position, side);
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: Side");
        }

        if (hasOrdType)
        {
            buffer.putBytes(position, ordTypeHeader, 0, ordTypeHeaderLength);
            position += ordTypeHeaderLength;
            position += buffer.putCharAscii(position, ordType);
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: OrdType");
        }

        if (hasPrice)
        {
            buffer.putBytes(position, priceHeader, 0, priceHeaderLength);
            position += priceHeaderLength;
            position += buffer.putFloatAscii(position, price);
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: Price");
        }

        position += orderQtyData.encode(buffer, position);

        if (transactTimeLength > 0)
        {
            buffer.putBytes(position, transactTimeHeader, 0, transactTimeHeaderLength);
            position += transactTimeHeaderLength;
            buffer.putBytes(position, transactTime, transactTimeOffset, transactTimeLength);
            position += transactTimeLength;
            buffer.putSeparator(position);
            position++;
        }
        else if (CODEC_VALIDATION_ENABLED)
        {
            throw new EncodingException("Missing Field: TransactTime");
        }

        position += trailer.startTrailer(buffer, position);

        final int messageStart = header.finishHeader(buffer, bodyStart, position - bodyStart);
        return trailer.finishMessage(buffer, messageStart, position);
    }

    @Override
    public void reset()
    {
        header.reset();
        trailer.reset();
        resetMessage();
    }

    @Override
    public void resetMessage()
    {
        clOrdIDLength     = 0; clOrdID.wrap(clOrdIDInternalBuffer);
        origClOrdIDLength = 0; origClOrdID.wrap(origClOrdIDInternalBuffer);
        side              = MISSING_CHAR;
        hasSide           = false;
        ordType           = MISSING_CHAR;
        hasOrdType        = false;
        hasPrice          = false;
        transactTimeLength = 0; transactTime.wrap(transactTimeInternalBuffer);
        instrument.reset();
        orderQtyData.reset();
    }

    @Override
    public Encoder copyTo(final Encoder encoder)
    {
        return this;   // TODO(POC): deep copy not needed for single-use POC encoder
    }

    @Override
    public String toString()
    {
        return appendTo(new StringBuilder()).toString();
    }

    @Override
    public StringBuilder appendTo(final StringBuilder builder)
    {
        builder.append("{\"MessageName\":\"OrderCancelReplaceRequest\"}");
        return builder;
    }
}
