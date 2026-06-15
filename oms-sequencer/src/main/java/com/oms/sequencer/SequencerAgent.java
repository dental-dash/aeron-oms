package com.oms.sequencer;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.sbe.MessageHeaderDecoder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * The Sequencer is the single point of total ordering in the OMS.
 *
 * <p>It reads from two pre-sequencer ingress channels, stamps each message with a
 * monotonically increasing sequence number, and republishes to the canonical streams:
 * <ul>
 *   <li>Command Ingress (StreamId 10) → Command Stream (StreamId 1)</li>
 *   <li>Event Ingress   (StreamId 11) → Event Stream   (StreamId 2)</li>
 * </ul>
 *
 * <p>The Sequencer contains NO business logic. It is purely a sequencing and fan-out
 * mechanism. All downstream components observe commands and events in the same
 * globally consistent order.
 *
 * <p>Threading: single AgentRunner thread — {@code nextSequence} needs no synchronisation.
 */
public class SequencerAgent implements Agent {

    private static final Log log = LogFactory.getLog(SequencerAgent.class);

    // Pre-allocated stamp buffer — reused for every message. No allocation on the hot path.
    private static final int BUFFER_SIZE = 4 * 1_024;
    private final UnsafeBuffer stampBuffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);

    // SBE MessageHeader = blockLength(2) + templateId(2) + schemaId(2) + version(2) = 8 bytes.
    // The sequence number field is the first field in every message body, immediately after
    // the header. We stamp it at this fixed offset without decoding the full message type.
    private static final int SEQ_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH; // = 8

    // Monotonic counter. Single-threaded — plain long is safe.
    private long nextSequence = 1L;

    private final Subscription ingressCommandSub;  // StreamId 10
    private final Subscription ingressEventSub;    // StreamId 11

    // Allocate fragment handlers once in the constructor — never inside doWork().
    private final FragmentHandler ingressCommandHandler;
    private final FragmentHandler ingressEventHandler;

    public SequencerAgent(
            Subscription ingressCommandSub,
            Subscription ingressEventSub,
            Publication  commandStreamPub,
            Publication  eventStreamPub) {
        this.ingressCommandSub = ingressCommandSub;
        this.ingressEventSub   = ingressEventSub;

        this.ingressCommandHandler = (buffer, offset, length, header) ->
                stamp(buffer, offset, length, commandStreamPub);
        this.ingressEventHandler = (buffer, offset, length, header) ->
                stamp(buffer, offset, length, eventStreamPub);
    }

    @Override
    public int doWork() {
        int work = 0;
        work += ingressCommandSub.poll(ingressCommandHandler, 10);
        work += ingressEventSub.poll(ingressEventHandler, 10);
        return work;
    }

    @Override
    public String roleName() { return "sequencer"; }

    @Override
    public void onStart() {
        log.info().append("sequencer started").commit();
    }

    @Override
    public void onClose() {
        log.info().append("sequencer closed").commit();
    }

    /**
     * Copies the message into the pre-allocated stamp buffer, overwrites the sequence number
     * field at the fixed SBE header boundary, then offers the stamped message to the
     * canonical publication.
     *
     * <p>IMPORTANT: {@code src} buffer is only valid for the duration of this callback.
     * We must copy the bytes into {@code stampBuffer} before returning.
     */
    private void stamp(DirectBuffer src, int offset, int length, Publication pub) {
        if (length > BUFFER_SIZE) {
            // Drop and warn — do not throw; a thrown exception kills the AgentRunner.
            log.warn()
                .append("Sequencer dropping oversized message length=").append(length)
                .append(" streamId=").append(pub.streamId())
                .commit();
            return;
        }

        // Copy src into our pre-allocated buffer (src is only valid during this callback).
        stampBuffer.putBytes(0, src, offset, length);

        // Stamp the sequence number at byte offset SEQ_OFFSET (immediately after the
        // 8-byte SBE MessageHeader). LITTLE_ENDIAN matches the SBE Java default.
        stampBuffer.putLong(SEQ_OFFSET, nextSequence, ByteOrder.LITTLE_ENDIAN);

        long result = pub.offer(stampBuffer, 0, length);
        if (result > 0) {
            log.debug()
                .append("[sequencer] stamped seq=").append(nextSequence)
                .append(" → streamId=").append(pub.streamId())
                .commit();
            nextSequence++;
        } else if (result == Publication.BACK_PRESSURED) {
            log.warn()
                .append("Sequencer back-pressured on streamId=").append(pub.streamId())
                .append(" seq=").append(nextSequence)
                .commit();
            // TODO(POC): add back-pressure idle strategy / retry budget
        } else if (result == Publication.NOT_CONNECTED) {
            log.warn()
                .append("Sequencer not connected on streamId=").append(pub.streamId())
                .commit();
        } else {
            log.error()
                .append("Sequencer offer failed on streamId=").append(pub.streamId())
                .append(" result=").append(result)
                .commit();
        }
    }
}
