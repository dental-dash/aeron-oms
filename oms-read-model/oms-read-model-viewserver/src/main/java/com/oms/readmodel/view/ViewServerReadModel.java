package com.oms.readmodel.view;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.common.OmsStreams;
import com.oms.sbe.CancelRejectedEventDecoder;
import com.oms.sbe.MessageHeaderDecoder;
import com.oms.sbe.OrderAcceptedEventDecoder;
import com.oms.sbe.OrderAmendedEventDecoder;
import com.oms.sbe.OrderCancelledEventDecoder;
import com.oms.sbe.OrderFilledEventDecoder;
import com.oms.sbe.OrderPartiallyFilledEventDecoder;
import com.oms.sbe.OrderRejectedEventDecoder;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Projects the sequenced Event Stream (StreamId 2) into an in-memory read model.
 *
 * <p>On startup, replays from the persisted file checkpoint position (0 if none) up to
 * the recorded Archive position, then tracks live image position and periodically writes
 * a binary checkpoint file for fast startup on next restart.
 *
 * <p>Each event is applied in sequence-number order (guaranteed by the Sequencer).
 * The {@code ConcurrentHashMap} allows a future HTTP/WebSocket thread to read
 * {@link #getOrders()} without blocking the AgentRunner thread.
 *
 * M5: OrderEventListener fires onOrderUpdate() after each orders.put().
 * TODO(POC): fsync before rename for production durability
 * TODO(POC): size term buffer based on max replay duration × msg rate
 */
public class ViewServerReadModel implements Agent {

    private static final Log log = LogFactory.getLog(ViewServerReadModel.class);

    private static final int    VIEW_REPLAY_STREAM_ID  = 21;
    private static final long   REPLAY_TIMEOUT_MS      = 10_000L;
    // Flush checkpoint every 100 processed fragments to bound replay work on restart
    private static final int    CHECKPOINT_INTERVAL    = 100;
    private static final String CHECKPOINT_FILE        = "view-server.checkpoint";

    // Pre-allocated decoders — never allocate inside doWork()
    private final MessageHeaderDecoder             headerDecoder    = new MessageHeaderDecoder();
    private final OrderAcceptedEventDecoder        acceptedDecoder  = new OrderAcceptedEventDecoder();
    private final OrderFilledEventDecoder          filledDecoder    = new OrderFilledEventDecoder();
    private final OrderRejectedEventDecoder        rejectedDecoder  = new OrderRejectedEventDecoder();
    private final OrderCancelledEventDecoder       cancelledDecoder = new OrderCancelledEventDecoder();
    private final OrderAmendedEventDecoder         amendedDecoder   = new OrderAmendedEventDecoder();
    private final OrderPartiallyFilledEventDecoder partialDecoder   = new OrderPartiallyFilledEventDecoder();
    private final CancelRejectedEventDecoder       cancelRejDecoder = new CancelRejectedEventDecoder();

    private final Subscription eventStreamSub;
    private final FragmentHandler fragmentHandler;
    private final Aeron        aeron;
    private final AeronArchive archive;

    // ConcurrentHashMap so a future query thread can read without locking the agent thread
    private final ConcurrentHashMap<Long, OrderView> orders = new ConcurrentHashMap<>();

    private volatile OrderEventListener listener = null;

    /** Called from main thread before AgentRunner.startOnThread(). Safe via volatile. */
    public void setListener(final OrderEventListener l) { this.listener = l; }

    private long  checkpointPosition       = 0L;
    private long  recordingId              = -1L;
    private int   fragmentsSinceCheckpoint = 0;
    private Image liveEventImage           = null;  // populated lazily on first fragment in doWork()

    public ViewServerReadModel(Subscription eventStreamSub, Aeron aeron, AeronArchive archive) {
        this.eventStreamSub  = eventStreamSub;
        this.aeron           = aeron;
        this.archive         = archive;
        this.fragmentHandler = this::onFragment;
    }

    /**
     * Loads the checkpoint position, then replays the Archive from that position
     * to the current recorded end. Leaves the orders map fully rebuilt before
     * doWork() begins processing live events.
     */
    @Override
    public void onStart() {
        log.info().append("[view-server] starting — loading checkpoint...").commit();
        checkpointPosition = loadCheckpoint();

        final long[] foundId           = {-1L};
        final long[] foundStopPosition = {AeronArchive.NULL_POSITION};

        final int found = archive.listRecordingsForUri(
                0, 1, OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM,
                (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    foundId[0]           = recId;
                    foundStopPosition[0] = stopPosition;  // NULL_POSITION if still active
                });

        if (found == 0 || foundId[0] < 0) {
            log.info().append("[view-server] no recording — starting live from scratch").commit();
            return;
        }
        recordingId = foundId[0];

        // For a stopped recording (restart), use the persisted stopPosition.
        // For an active recording (same boot), fall back to the live position.
        long currentPos = foundStopPosition[0];
        if (currentPos == AeronArchive.NULL_POSITION) {
            currentPos = archive.getRecordingPosition(recordingId);
        }

        if (currentPos == AeronArchive.NULL_POSITION || currentPos <= checkpointPosition) {
            log.info().append("[view-server] recording not beyond checkpoint — no replay needed").commit();
            return;
        }

        log.info().append("[view-server] replaying from=").append(checkpointPosition)
            .append(" to=").append(currentPos).commit();

        final long replaySessionId = archive.startReplay(
                recordingId, checkpointPosition, currentPos - checkpointPosition,
                OmsStreams.IPC, VIEW_REPLAY_STREAM_ID);

        try (final Subscription replaySub = aeron.addSubscription(
                     OmsStreams.IPC, VIEW_REPLAY_STREAM_ID)) {

            final long deadlineMs = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            Image replayImage = null;
            while (replayImage == null) {
                replayImage = replaySub.imageBySessionId((int) replaySessionId);
                if (System.currentTimeMillis() > deadlineMs) {
                    throw new IllegalStateException("[view-server] replay image connect timeout");
                }
                Thread.yield();
            }

            // Poll until the image closes — Archive closes it after delivering requested bytes.
            while (!replayImage.isClosed()) {
                final int frags = replayImage.poll(fragmentHandler, 256);  // reuses onFragment
                if (frags == 0) Thread.yield();
            }
        }

        checkpointPosition = currentPos;
        saveCheckpoint(checkpointPosition);
        log.info().append("[view-server] replay complete — orders=").append(orders.size()).commit();
    }

    /**
     * Polls the live event stream and periodically flushes the checkpoint.
     * The checkpoint is the live image position, not a fragment count — this ensures
     * we never record a position that corresponds to a partial message.
     */
    @Override
    public int doWork() {
        final int fragments = eventStreamSub.poll(fragmentHandler, 10);
        if (fragments > 0) {
            // Lazily capture the image reference so we can read its position
            if (liveEventImage == null && eventStreamSub.imageCount() > 0) {
                liveEventImage = eventStreamSub.imageAtIndex(0);
            }
            if (liveEventImage != null) {
                fragmentsSinceCheckpoint += fragments;
                if (fragmentsSinceCheckpoint >= CHECKPOINT_INTERVAL) {
                    checkpointPosition = liveEventImage.position();
                    saveCheckpoint(checkpointPosition);
                    fragmentsSinceCheckpoint = 0;
                }
            }
        }
        return fragments;
    }

    /** Final checkpoint flush on clean shutdown (Ctrl-C → shutdown hook → onClose). */
    @Override
    public void onClose() {
        if (liveEventImage != null) {
            checkpointPosition = liveEventImage.position();
        }
        saveCheckpoint(checkpointPosition);
        log.info().append("[view-server] closed — checkpoint=").append(checkpointPosition).commit();
    }

    @Override
    public String roleName() { return "view-server"; }

    /** Read-only view for query threads (M5 REST/WebSocket layer). */
    public Map<Long, OrderView> getOrders() {
        return Collections.unmodifiableMap(orders);
    }

    // ── Checkpoint file helpers ───────────────────────────────────────────────

    private long loadCheckpoint() {
        final Path p = Paths.get(CHECKPOINT_FILE);
        if (!Files.exists(p)) return 0L;
        try {
            final byte[] b = Files.readAllBytes(p);
            return (b.length >= Long.BYTES)
                    ? ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong()
                    : 0L;
        } catch (IOException e) {
            log.warn().append("[view-server] failed to read checkpoint: ").append(e.getMessage()).commit();
            return 0L;
        }
    }

    /** Atomic write via tmp-then-rename to avoid partial file on crash. */
    private void saveCheckpoint(final long position) {
        final Path target = Paths.get(CHECKPOINT_FILE);
        final Path tmp    = Paths.get(CHECKPOINT_FILE + ".tmp");
        try {
            final byte[] b = new byte[Long.BYTES];
            ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).putLong(position);
            Files.write(tmp, b, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            // ATOMIC_MOVE is best-effort on some filesystems — acceptable for POC
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn().append("[view-server] checkpoint save failed: ").append(e.getMessage()).commit();
        }
    }

    // ── Fragment handler ──────────────────────────────────────────────────────

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch (templateId) {
            case OrderAcceptedEventDecoder.TEMPLATE_ID          -> handleOrderAccepted(buffer, offset);
            case OrderFilledEventDecoder.TEMPLATE_ID            -> handleOrderFilled(buffer, offset);
            case OrderRejectedEventDecoder.TEMPLATE_ID          -> handleOrderRejected(buffer, offset);
            case OrderCancelledEventDecoder.TEMPLATE_ID         -> handleOrderCancelled(buffer, offset);
            case OrderAmendedEventDecoder.TEMPLATE_ID           -> handleOrderAmended(buffer, offset);
            case OrderPartiallyFilledEventDecoder.TEMPLATE_ID   -> handleOrderPartiallyFilled(buffer, offset);
            case CancelRejectedEventDecoder.TEMPLATE_ID         -> handleCancelRejected(buffer, offset);
        }
    }

    private void handleOrderAccepted(DirectBuffer buffer, int offset) {
        acceptedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final OrderView view = new OrderView(
                acceptedDecoder.orderId(),
                acceptedDecoder.accountId(),
                acceptedDecoder.instrument(),
                acceptedDecoder.side().name(),
                acceptedDecoder.orderType().name(),
                acceptedDecoder.timeInForce().name(),
0L, 0L,
//                acceptedDecoder.price(),
//                acceptedDecoder.quantity(),
                OrderStatus.OPEN);

        orders.put(view.orderId, view);
        final OrderEventListener l0 = listener;
        if (l0 != null) l0.onOrderUpdate(view.orderId, view);
        log.info().append("[view-server] ").append(view.toString()).commit();
    }

    private void handleOrderFilled(DirectBuffer buffer, int offset) {
        filledDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long orderId = filledDecoder.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderFilledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withFill(filledDecoder.fillPrice(), filledDecoder.fillQuantity());
        orders.put(orderId, updated);
        final OrderEventListener l1 = listener;
        if (l1 != null) l1.onOrderUpdate(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    private void handleOrderRejected(DirectBuffer buffer, int offset) {
        rejectedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        // Order never reached OPEN state — no view entry to update
        log.info()
            .append("[view-server] OrderRejectedEvent orderId=").append(rejectedDecoder.orderId())
            .append(" reason=").append(rejectedDecoder.rejectReason().name())
            .commit();
    }

    private void handleOrderCancelled(DirectBuffer buffer, int offset) {
        cancelledDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long orderId = cancelledDecoder.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderCancelledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withCancel();
        orders.put(orderId, updated);
        final OrderEventListener l2 = listener;
        if (l2 != null) l2.onOrderUpdate(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    private void handleOrderAmended(DirectBuffer buffer, int offset) {
        amendedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long orderId = amendedDecoder.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderAmendedEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withAmend(amendedDecoder.newPrice(), amendedDecoder.newQuantity());
        orders.put(orderId, updated);
        final OrderEventListener l3 = listener;
        if (l3 != null) l3.onOrderUpdate(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    private void handleOrderPartiallyFilled(DirectBuffer buffer, int offset) {
        partialDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long orderId = partialDecoder.orderId();
        final OrderView existing = orders.get(orderId);
        if (existing == null) {
            log.warn()
                .append("[view-server] OrderPartiallyFilledEvent for unknown orderId=").append(orderId)
                .commit();
            return;
        }

        final OrderView updated = existing.withPartialFill(
                partialDecoder.fillPrice(), partialDecoder.fillQuantity(), partialDecoder.remainingQty());
        orders.put(orderId, updated);
        final OrderEventListener l4 = listener;
        if (l4 != null) l4.onOrderUpdate(orderId, updated);
        log.info().append("[view-server] ").append(updated.toString()).commit();
    }

    private void handleCancelRejected(DirectBuffer buffer, int offset) {
        cancelRejDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        // Order view unchanged — just log the rejection
        log.info()
            .append("[view-server] CancelRejectedEvent orderId=").append(cancelRejDecoder.orderId())
            .append(" reason=").append(cancelRejDecoder.rejectReason().name())
            .append(" (order unchanged)")
            .commit();
    }
}
