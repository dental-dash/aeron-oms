package com.oms.fix.acceptor;

import com.oms.fix.sbe.*;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.decoder.NewOrderSingleDecoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * M3: Full SessionHandler — decodes Artio FIX NewOrderSingle → SBE-encodes NewOrderSingleCommand
 * → publishes to Aeron IPC stream 10 (COMMAND_INGRESS_STREAM).
 */
public final class FixSessionHandler implements SessionHandler
{
    // SBE encoded length: 8 (MessageHeader) + 120 (block) = 128 bytes
    private static final int SBE_ENCODED_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixNewOrderSingleCommandEncoder.BLOCK_LENGTH;

    private final CommandStreamPublisher publisher;
    private final ConcurrentHashMap<Long, Session> activeSessions;

    // Artio decoder — reuse per-instance (single-threaded Artio library polling)
    private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();
    // Wrapper to present Aeron DirectBuffer as Artio AsciiBuffer for decoding
    private final MutableAsciiBuffer asciiWrapper = new MutableAsciiBuffer();

    // SBE encoder — reuse per-instance
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final FixNewOrderSingleCommandEncoder sbeEncoder = new FixNewOrderSingleCommandEncoder();
    private final UnsafeBuffer sendBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(SBE_ENCODED_LENGTH));

    public FixSessionHandler(final CommandStreamPublisher publisher,
                             final ConcurrentHashMap<Long, Session> activeSessions)
    {
        this.publisher       = publisher;
        this.activeSessions  = activeSessions;
    }

    @Override
    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final Session session,
        final int sequenceIndex,
        final long messageType,
        final long timestampInNs,
        final long position,
        final OnMessageInfo messageInfo)
    {
        // DIAGNOSTIC: log every inbound application message before filtering.
        // TODO(POC): remove after M3 pipeline is confirmed working.
        System.out.printf("[FixSession] onMessage: msgType=%d (nosType=%d) len=%d%n",
            messageType, NewOrderSingleDecoder.MESSAGE_TYPE, length);

        if (messageType != NewOrderSingleDecoder.MESSAGE_TYPE)
        {
            return Action.CONTINUE;
        }

        // Wrap the Aeron DirectBuffer slice so Artio's decoder can parse ASCII FIX bytes.
        asciiWrapper.wrap(buffer, offset, length);
        nosDecoder.decode(asciiWrapper, 0, length);

        // Map FIX 4.4 Side char → SBE SideEnum (FIX '1'=BUY, '2'=SELL)
        final SideEnum side = nosDecoder.side() == '1' ? SideEnum.BUY : SideEnum.SELL;
        // Map FIX 4.4 OrdType char → SBE OrdTypeEnum (FIX '1'=MARKET, '2'=LIMIT)
        final OrdTypeEnum ordType = nosDecoder.ordType() == '2' ? OrdTypeEnum.LIMIT : OrdTypeEnum.MARKET;

        final DecimalFloat price    = nosDecoder.price();
        final DecimalFloat orderQty = nosDecoder.orderQty();

        // SBE-encode: wrapAndApplyHeader writes the 8-byte header then positions encoder after it.
        sbeEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder)
            .sequenceNumber(0L)               // Sequencer stamps in a later milestone
            .sessionId(session.id())
            .clOrdId(nosDecoder.clOrdIDAsString())
            .symbol(nosDecoder.symbolAsString())
            .side(side)
            .ordType(ordType)
            .account("");                     // FIX tag 1 optional in M3

        // Artio DecimalFloat: value()=mantissa, scale()=decimal places (positive).
        // SBE Decimal64: mantissa × 10^exponent, so exponent = -scale.
        sbeEncoder.price()
            .mantissa(price.value())
            .exponent((byte) -price.scale());

        sbeEncoder.orderQty()
            .mantissa(orderQty.value())
            .exponent((byte) -orderQty.scale());

        sbeEncoder.transactTime(timestampInNs);

        publisher.offer(sendBuffer, 0, SBE_ENCODED_LENGTH);

        System.out.printf("[FixSession] Published NOS → stream10: clOrdId=%s symbol=%s side=%s%n",
            nosDecoder.clOrdIDAsString(), nosDecoder.symbolAsString(), side);

        return Action.CONTINUE;
    }

    @Override
    public void onTimeout(final int libraryId, final Session session)
    {
        System.out.printf("[Acceptor] Session timeout: libraryId=%d sessionId=%d%n",
            libraryId, session.id());
    }

    @Override
    public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
    {
        System.out.printf("[Acceptor] Slow-consumer: hasBecomeSlow=%b sessionId=%d%n",
            hasBecomeSlow, session.id());
    }

    @Override
    public void onSessionStart(final Session session)
    {
        final CompositeKey key = session.compositeKey();
        System.out.printf("[Acceptor] Logon complete: local=%s remote=%s sessionId=%d%n",
            key.localCompId(), key.remoteCompId(), session.id());
    }

    @Override
    public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
    {
        activeSessions.remove(session.id());
        System.out.printf("[Acceptor] Disconnected: sessionId=%d reason=%s (removed from activeSessions)%n",
            session.id(), reason);
        return Action.CONTINUE;
    }
}
