package com.oms.fix.acceptor;

import com.oms.fix.sbe.MessageHeaderDecoder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * M4: Command-only sequencer for the FIX integration process.
 *
 * <p>Subscribes to the Command Ingress Stream (IPC stream {@value com.oms.common.OmsStreams#INGRESS_COMMAND_STREAM}),
 * stamps each message with a monotonically increasing {@code sequenceNumber} at the fixed
 * SBE header boundary (byte offset {@value SEQ_OFFSET}), then republishes to the
 * Sequenced Command Stream (IPC stream {@value com.oms.common.OmsStreams#SEQUENCED_COMMAND_STREAM}).
 *
 * <p>Contains NO business logic — purely a sequencing pass-through. The sequenceNumber
 * field is the first field in every FIX SBE message body (immediately after the 8-byte
 * messageHeader composite), so this agent stamps any template without decoding it.
 *
 * <p>Single-threaded via AgentRunner — {@code nextSequence} needs no synchronisation.
 */
public final class FixSequencerAgent implements Agent
{
    // Pre-allocated stamp buffer — reused for every fragment, zero allocation on hot path.
    private static final int BUFFER_SIZE = 4 * 1_024;
    private final UnsafeBuffer stampBuffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);

    // sequenceNumber is field id=1 in every FIX SBE message, at byte offset 8 immediately
    // after the 8-byte messageHeader composite. Identical invariant to the main OMS sequencer.
    static final int SEQ_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH; // = 8

    // Monotonically increasing counter. Single-threaded — plain long is safe.
    private long nextSequence = 1L;

    private final Subscription commandIngressSub; // IPC stream 10 (raw, from acceptor)
    private final Publication  commandStreamPub;  // IPC stream 1  (sequenced)

    public FixSequencerAgent(final Subscription commandIngressSub,
                             final Publication  commandStreamPub)
    {
        this.commandIngressSub = commandIngressSub;
        this.commandStreamPub  = commandStreamPub;
    }

    @Override
    public int doWork()
    {
        return commandIngressSub.poll(this::stamp, 10);
    }

    @Override
    public String roleName() { return "fix-sequencer"; }

    @Override
    public void onStart()
    {
        System.out.println("[Sequencer] started — stream10 → stamp → stream1");
    }

    @Override
    public void onClose()
    {
        System.out.println("[Sequencer] closed");
    }

    /**
     * Copies the fragment into the pre-allocated stamp buffer, overwrites the
     * sequenceNumber field at {@link #SEQ_OFFSET}, then offers the stamped
     * message to the sequenced command publication.
     *
     * <p>IMPORTANT: {@code src} is only valid for the duration of this callback —
     * we must copy the bytes before returning.
     */
    private void stamp(final DirectBuffer src, final int offset, final int length,
                       final Header header)
    {
        if (length > BUFFER_SIZE)
        {
            System.err.printf("[Sequencer] Dropping oversized message length=%d%n", length);
            return;
        }

        // Copy src into our pre-allocated buffer.
        stampBuffer.putBytes(0, src, offset, length);

        // Stamp the sequence number at the fixed offset. LITTLE_ENDIAN matches SBE Java default.
        stampBuffer.putLong(SEQ_OFFSET, nextSequence, ByteOrder.LITTLE_ENDIAN);

        final long result = commandStreamPub.offer(stampBuffer, 0, length);
        if (result > 0)
        {
            System.out.printf("[Sequencer] seq=%d → stream%d%n",
                nextSequence, commandStreamPub.streamId());
            nextSequence++;
        }
        else if (result == Publication.BACK_PRESSURED)
        {
            // TODO(POC): add retry budget; production should spin with bounded attempts
            System.err.printf("[Sequencer] back-pressured seq=%d dropped%n", nextSequence);
        }
        else if (result == Publication.NOT_CONNECTED)
        {
            // No subscriber on stream 1 yet — expected if dumper not started
            System.err.printf("[Sequencer] not connected seq=%d dropped%n", nextSequence);
        }
        else
        {
            System.err.printf("[Sequencer] offer failed result=%d seq=%d dropped%n",
                result, nextSequence);
        }
    }
}
