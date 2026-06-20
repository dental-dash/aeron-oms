package com.oms.app;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.aggregate.client.OrderDomainService;
import com.oms.aggregate.client.generated.OrderDomainServiceAgent;
import com.oms.common.OmsStreams;
import com.oms.handlers.FixOrderEventHandler;
import com.oms.handlers.generated.FixOrderEventHandlerAgent;
import com.oms.readmodel.db.DatabaseReadModelStub;
import com.oms.readmodel.view.ViewServerEventHandler;
import com.oms.readmodel.view.generated.ViewServerEventHandlerAgent;
import com.oms.sequencer.SequencerAgent;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.List;

/**
 * OMS POC bootstrap — single-process entry point.
 *
 * <p>Connects to the shared {OmsMediaDriverMain} at {@code /tmp/aeron-oms}
 * (must be running before this process starts), wires all Aeron publications and subscriptions,
 * instantiates each component as an {@link org.agrona.concurrent.Agent}, and runs each on its
 * own dedicated {@link AgentRunner} thread.
 *
 * TODO(POC): use dedicated Aeron for archive control to isolate back-pressure
 * TODO(POC): add secondary Archive node for HA
 */
public class OmsApp {
    private static final Log log = LogFactory.getLog(OmsApp.class);
    // Shared Aeron dir — must match OmsMediaDriverMain.AERON_DIR
    private static final String AERON_DIR = "/tmp/aeron-oms";
    // Archive control on OmsMediaDriverMain's fixed port
    private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8010";

    public static void main(String[] args) throws InterruptedException {

        // ── 1. Connect to shared MediaDriver ─────────────────────────────────
        // OmsMediaDriverMain must be running before this process starts.
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));

        // ── 2. Connect AeronArchive client ────────────────────────────────────
        // Connects to the Archive running inside OmsMediaDriverMain on port 8010.
        // Ephemeral response port (:0) is safe here — OmsApp is not Artio and does
        // not have the fixed-port requirement that Artio's archive client has.
        final AeronArchive archive = AeronArchive.connect(
                new AeronArchive.Context()
                        .aeron(aeron)
                        .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));

        // ── 3. Start recordings BEFORE any publications ───────────────────────

        // Check and start Command Stream recording
        long commandRecordingId = findActiveRecordingId(archive, OmsStreams.SEQUENCED_COMMAND_STREAM);
        if (commandRecordingId == AeronArchive.NULL_POSITION) {
            archive.startRecording(OmsStreams.IPC, OmsStreams.SEQUENCED_COMMAND_STREAM, SourceLocation.LOCAL);
            log.info().append("Started new recording for SEQUENCED_COMMAND_STREAM").commit();
        } else {
            log.info().append("Re-using active recording ID ").append(commandRecordingId).append(" for SEQUENCED_COMMAND_STREAM").commit();
        }

        // Check and start Event Stream recording
        long eventRecordingId = findActiveRecordingId(archive, OmsStreams.SEQUENCED_EVENT_STREAM);
        if (eventRecordingId == AeronArchive.NULL_POSITION) {
            archive.startRecording(OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM, SourceLocation.LOCAL);
            log.info().append("Started new recording for SEQUENCED_EVENT_STREAM").commit();
        } else {
            log.info().append("Re-using active recording ID ").append(eventRecordingId).append(" for SEQUENCED_EVENT_STREAM").commit();
        }

        // ── 4. Sequencer channels ─────────────────────────────────────────────
        final Subscription ingressCommandSub = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.INGRESS_COMMAND_STREAM);
        final Subscription ingressEventSub   = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.INGRESS_EVENT_STREAM);
        final Publication sequencedCommandPub   = aeron.addPublication(
                OmsStreams.IPC, OmsStreams.SEQUENCED_COMMAND_STREAM);
        final Publication sequencedEventPub     = aeron.addPublication(
                OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);

        // ── 5. Component channels ─────────────────────────────────────────────
        final Publication ingressCommandPub1  = aeron.addPublication(
                OmsStreams.IPC, OmsStreams.INGRESS_COMMAND_STREAM);

        // OrderAggregateAgent and FillSimulatorHandler each get their own Publication on
        // Event Ingress (StreamId 11). Aeron IPC allows multiple concurrent publishers.
        final Publication ingressEventPub1   = aeron.addPublication(
                OmsStreams.IPC, OmsStreams.INGRESS_EVENT_STREAM);  // used by OrderAggregateAgent
        final Publication ingressEventPub2   = aeron.addPublication(
                OmsStreams.IPC, OmsStreams.INGRESS_EVENT_STREAM);  // used by FillSimulatorHandler

        // Each downstream subscriber gets its own independent subscription position.
        final Subscription sequencedCommandPub1         = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.SEQUENCED_COMMAND_STREAM);
        final Subscription sequencedEventSub1  = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);   // OrderAggregateAgent (observes fills)
        final Subscription sequencedEventSub2        = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);   // FillSimulatorHandler
        final Subscription sequencedEventSub3          = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);   // DatabaseReadModelStub
        final Subscription sequencedEventSub4        = aeron.addSubscription(
                OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);   // ViewServerReadModel

        // ── 6. Agents ─────────────────────────────────────────────────────────
        final SequencerAgent        sequencer  = new SequencerAgent(
                ingressCommandSub, ingressEventSub, sequencedCommandPub, sequencedEventPub);
//        final OrderIngressAgent     ingress    = new OrderIngressAgent(commandIngressPub);
        // Aggregate replays the Event Stream on startup to rebuild in-memory order state.
        // TODO(POC): size term buffer based on max replay duration × msg rate
        // OrderDomainService
        final OrderDomainServiceAgent orderDomainServiceAgent = new OrderDomainServiceAgent(
                sequencedCommandPub1, ingressEventPub1, aeron, archive);
        final OrderDomainService orderDomainService = new OrderDomainService(orderDomainServiceAgent);
        orderDomainServiceAgent.setOrderDomainService(orderDomainService);

        // FixOrderEventHandler
        final FixOrderEventHandlerAgent fixOrderEventServiceAgent = new FixOrderEventHandlerAgent(sequencedEventSub2, ingressCommandPub1);
        final FixOrderEventHandler fixOrderEventService    = new FixOrderEventHandler(fixOrderEventServiceAgent);
        fixOrderEventServiceAgent.setFixOrderEventService(fixOrderEventService);

        //
        final DatabaseReadModelStub dbModel    = new DatabaseReadModelStub(sequencedEventSub3);

        // ViewServerEventHandler
        final ViewServerEventHandlerAgent viewServerEventHandlerAgent = new ViewServerEventHandlerAgent(sequencedEventSub4, aeron, archive);
        final ViewServerEventHandler viewServerEventHandler = new ViewServerEventHandler(viewServerEventHandlerAgent);
        viewServerEventHandlerAgent.setViewServerEventHandler(viewServerEventHandler);

//        final ViewServerReadModel   viewModel  = new ViewServerReadModel(
//                sequencedEventSub4, aeron, archive);

        // ── M5: Query server (port 8081) ──────────────────────────────────────

        // ── 7. AgentRunner threads ────────────────────────────────────────────
        // YieldingIdleStrategy: backs off with Thread.yield() when idle. Good balance of
        // latency vs CPU for a POC. Swap to BusySpinIdleStrategy for minimal latency in prod.
        final List<AgentRunner> runners = List.of(
                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, sequencer),
//                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, ingress),
                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, orderDomainServiceAgent),
                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, fixOrderEventServiceAgent),
                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, dbModel),
                new AgentRunner(new YieldingIdleStrategy(), Throwable::printStackTrace, null, viewServerEventHandlerAgent)
        );

        runners.forEach(AgentRunner::startOnThread);

        // ── 8. Shutdown hook ──────────────────────────────────────────────────
        // Order matters: close agents first (onClose() writes ViewServer checkpoint),
        // then archive, then aeron. Driver is managed by OmsMediaDriverMain.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runners.forEach(r -> CloseHelper.quietClose(r));  // onClose() writes checkpoint
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }, "oms-shutdown"));

        // ── 9. Block main thread ──────────────────────────────────────────────
        Thread.currentThread().join();
    }

    private static long findActiveRecordingId(AeronArchive archive, int streamId) {
        final long[] activeRecordingId = {AeronArchive.NULL_POSITION};

        // Query by streamId broadly to bypass exact URI string matching quirks
        archive.listRecordingsForUri(0, 50, "", streamId,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamIdProp, strippedChannel, originalChannel, sourceIdentity) -> {

                    // If stopPosition is NULL_POSITION (-1), the Archive is actively recording it
                    if (stopPosition == AeronArchive.NULL_POSITION && streamIdProp == streamId) {
                        // Ensure it belongs to our IPC channel space
                        if (originalChannel.contains("aeron:ipc") || strippedChannel.contains("aeron:ipc")) {
                            activeRecordingId[0] = recordingId;
                        }
                    }
                });

        return activeRecordingId[0];
    }
}
