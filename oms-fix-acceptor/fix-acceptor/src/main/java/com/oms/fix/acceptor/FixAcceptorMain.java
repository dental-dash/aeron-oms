package com.oms.fix.acceptor;

import com.oms.common.OmsStreams;
import com.oms.aggregate.fix.FixOrderAggregateAgent;
import com.oms.aggregate.fix.FixOrderState;
import com.oms.fix.sbe.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.YieldingIdleStrategy;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FIX acceptor — listens on TCP port 9880 and bridges FIX orders into the OMS.
 *
 * <p>Uses TWO Aeron connections:
 * <ol>
 *   <li>Artio internal ({@value #ARTIO_AERON_DIR}) — Artio's engine↔library IPC and its own
 *       archive for FIX message replay. Backed by an embedded {@link ArchivingMediaDriver}.
 *       Artio creates its Aeron connections via {@code System.setProperty("aeron.dir", ...)}
 *       which is set to this dir.
 *   <li>OMS IPC ({@code /tmp/aeron-oms}) — shared with {@link com.oms.driver.OmsMediaDriverMain}
 *       and OmsApp. Used exclusively for OMS publications/subscriptions. Connected via explicit
 *       {@code aeronDirectoryName("/tmp/aeron-oms")} — unaffected by the system property.
 * </ol>
 *
 * <p>Startup order: OmsMediaDriverMain → OmsApp → this → FixClientMain.
 * OmsApp must be running before orders flow because it hosts the sole SequencerAgent.
 *
 * <p>Message flow:
 * <pre>
 *   FIX NOS → FixSessionHandler → COMMAND_INGRESS(10) [via omsAeron]
 *          → OmsApp.SequencerAgent stamps → COMMAND_STREAM(1)
 *          → FixOrderAggregateAgent translates NOS→PlaceOrder → COMMAND_INGRESS(10)
 *          → OmsApp.SequencerAgent stamps → COMMAND_STREAM(1)
 *          → OmsApp.OrderAggregateAgent → OrderAcceptedEvent → EVENT_STREAM(2)
 * </pre>
 *
 * <p>There is intentionally NO FixSequencerAgent in this process. OmsApp's SequencerAgent
 * is the sole sequencer — running two would double-stamp every message.
 */
public class FixAcceptorMain
{
    // Artio engine↔library channel — IPC within this process only (Artio's own Aeron dir).
    static final String LIBRARY_CHANNEL = "aeron:ipc";
    static final int FIX_PORT = 9880;
    private static final String LOG_DIR = "./fix-acceptor-logs";

    // ── Artio's private Aeron dir + embedded archive ──────────────────────────
    // Completely isolated from the shared OMS driver. Artio uses this for internal
    // engine↔library IPC and for recording FIX messages for resend/replay.
    private static final String ARTIO_AERON_DIR  = "./aeron-fix-acceptor";
    private static final String ARTIO_ARCHIVE_DIR = "./fix-acceptor-archive";
    // Fixed ports for Artio's archive — no collision with OMS (8010) or client (8020).
    private static final String ARTIO_ARCHIVE_CONTROL     = "aeron:udp?endpoint=localhost:8030";
    private static final String ARTIO_ARCHIVE_RESPONSE    = "aeron:udp?endpoint=localhost:8031";
    private static final String ARTIO_ARCHIVE_REPLICATION = "aeron:udp?endpoint=localhost:0";

    // ── Shared OMS driver dir ─────────────────────────────────────────────────
    // Used only for OMS IPC pub/sub (commandPub, fixAggCommandSub, etc.).
    // Does NOT go through Artio — connected via explicit aeronDirectoryName().
    private static final String OMS_AERON_DIR = "/tmp/aeron-oms";

    public static void main(final String[] args) throws Exception
    {
        final String artioAeronDirAbsolute = Paths.get(ARTIO_AERON_DIR).toAbsolutePath().normalize().toString();

        // System property controls where Artio's internal Aeron.connect() looks for the driver.
        // MUST point to Artio's own dir, not the shared OMS dir — otherwise Artio's engine↔library
        // IPC would land on the shared driver and conflict with other Artio processes.
        System.setProperty("aeron.dir", artioAeronDirAbsolute);

        // ── Step 1: Embedded ArchivingMediaDriver for Artio ───────────────────
        // Artio's FixEngine calls Aeron.connect() internally — it needs a running driver.
        // ThreadingMode.DEDICATED: gives the conductor its own thread so its CnC heartbeat
        // is always updated promptly. SHARED shares one thread with sender+receiver — on a
        // loaded system with many JVMs competing, the conductor can miss its 10s keepalive
        // window and trigger DriverTimeoutException during FixEngine startup.
        final ArchivingMediaDriver artioDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(artioAeronDirAbsolute)
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED),
            new Archive.Context()
                .archiveDir(new File(ARTIO_ARCHIVE_DIR))
                .deleteArchiveOnStart(true)
                .controlChannel(ARTIO_ARCHIVE_CONTROL)
                .replicationChannel(ARTIO_ARCHIVE_REPLICATION)
                .recordingEventsEnabled(false)
                .aeronDirectoryName(artioAeronDirAbsolute)
        );

        // ── Step 2: Separate Aeron connection to the shared OMS driver ────────
        // Explicit aeronDirectoryName() overrides System.setProperty — connects to OmsMediaDriverMain.
        // All OMS pub/sub use this instance; Artio uses the one above via System.setProperty.
        final Aeron omsAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(OMS_AERON_DIR));

        // Acceptor publishes NOS to COMMAND_INGRESS; OmsApp.SequencerAgent reads and stamps it.
        final Publication ingressCommandPub = omsAeron.addPublication(OmsStreams.IPC, OmsStreams.INGRESS_COMMAND_STREAM);
        final CommandStreamPublisher publisher = new CommandStreamPublisher(ingressCommandPub);

        // FixOrderAggregateAgent reads from COMMAND_STREAM(1) — after OmsApp's sequencer has
        // stamped the NOS. It translates NOS→PlaceOrder and publishes back to COMMAND_INGRESS(10).
        // OmsApp must be running for messages to appear on COMMAND_STREAM(1).
        final Subscription sequencedCommandSub   = omsAeron.addSubscription(OmsStreams.IPC, OmsStreams.SEQUENCED_COMMAND_STREAM);
//        final Publication  ingressCommandPub = omsAeron.addPublication(OmsStreams.IPC, OmsStreams.INGRESS_COMMAND_STREAM);
        final Publication ingressEventPub = omsAeron.addPublication(OmsStreams.IPC, OmsStreams.INGRESS_EVENT_STREAM);

        // ── Shared correlation maps ────────────────────────────────────────────
        // fixStateByOrderId: written by FixOrderAggregateAgent (agent thread),
        //                    read by FixExecReportBridge (Artio poll thread).
        // activeSessions:    written+read by FixSessionHandler and FixExecReportBridge
        //                    (both on Artio poll thread — ConcurrentHashMap for visibility).
        final ConcurrentHashMap<Long, FixOrderState> fixStateByOrderId = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Long, Session>       activeSessions    = new ConcurrentHashMap<>();

        final FixOrderAggregateAgent fixAggAgent = new FixOrderAggregateAgent(
            sequencedCommandSub, ingressEventPub, fixStateByOrderId);


        final AgentRunner fixAggRunner = new AgentRunner(
            new YieldingIdleStrategy(), Throwable::printStackTrace, null, fixAggAgent);
        AgentRunner.startOnThread(fixAggRunner);

        // Subscribe to EVENT_STREAM(2) for FixExecReportBridge — polled on the main loop thread
        // (same thread as fixLibrary.poll()) to keep Artio session.trySend() single-threaded.
        final Subscription eventStreamSub = omsAeron.addSubscription(OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);
        final FixExecReportBridge execReportBridge = new FixExecReportBridge(fixStateByOrderId, activeSessions);

        // M7 verification — two independent check points on the shared OMS streams.
        final Subscription         verifyNewOrderSub = omsAeron.addSubscription(OmsStreams.IPC, OmsStreams.SEQUENCED_COMMAND_STREAM);
        final Subscription         verifyEventSub      = omsAeron.addSubscription(OmsStreams.IPC, OmsStreams.SEQUENCED_EVENT_STREAM);
        final MessageHeaderDecoder verifyHeaderDecoder = new MessageHeaderDecoder();

        // ── Step 3: Configure FixEngine with Artio's own archive ─────────────
        // aeronContext: pass explicit dir + 30s timeout to Artio's internal Aeron client.
        // Belt-and-suspenders alongside DEDICATED threading — handles any residual slow startup.
        final EngineConfiguration engineCfg = new EngineConfiguration()
            .bindTo("0.0.0.0", FIX_PORT)
            .libraryAeronChannel(LIBRARY_CHANNEL)
            .logFileDir(LOG_DIR)
            .deleteLogFileDirOnStart(true)
            .scheduler(new LowResourceEngineScheduler());

        // aeronContext() returns the existing Aeron.Context stored inside EngineConfiguration.
        // Mutate it in place — Artio 0.153 has no setter, only the getter.
        engineCfg.aeronContext().driverTimeoutMs(30_000L);

        engineCfg.aeronArchiveContext()
            .controlRequestChannel(ARTIO_ARCHIVE_CONTROL)   // Artio's own archive — NOT the shared OMS archive
            .controlResponseChannel(ARTIO_ARCHIVE_RESPONSE)
            .aeronDirectoryName(artioAeronDirAbsolute);

        // ── Step 4: Launch engine (connects to artioDriver via System.setProperty) ──
        final FixEngine engine = FixEngine.launch(engineCfg);

        // ── Step 5: Connect FixLibrary ────────────────────────────────────────
        final FixSessionAcquireHandler acquireHandler = new FixSessionAcquireHandler(publisher, activeSessions);

        final LibraryConfiguration libraryCfg = new LibraryConfiguration()
            .sessionAcquireHandler(acquireHandler)
            .sessionExistsHandler(
                (lib, surrogateId, localCompId, localSubId, localLocationId,
                 remoteCompId, remoteSubId, remoteLocationId, logonSeqNum, seqIndex) ->
                {
                    System.out.printf("[Acceptor] Existing session: remote=%s — requesting acquisition%n", remoteCompId);
                    lib.requestSession(surrogateId, -1, seqIndex, 5_000L);
                })
            .libraryAeronChannels(Collections.singletonList(LIBRARY_CHANNEL));

        final FixLibrary library = FixLibrary.connect(libraryCfg);

        // ── Step 6: Wait for OmsApp pipeline to be ready ─────────────────────
        // commandPub (stream 10) is NOT_CONNECTED until OmsApp's commandIngressSub is live.
        // internalCommandPub likewise needs OmsApp's sequencer subscription.
        // Without this wait, the first NOS arrives before OmsApp is ready and is silently dropped.
        System.out.println("[Acceptor] Waiting for OmsApp commandIngress connection (stream 10)...");
        final long connectionDeadline = System.currentTimeMillis() + 30_000L;
        while (!ingressCommandPub.isConnected() || !ingressCommandPub.isConnected())
        {
            if (System.currentTimeMillis() > connectionDeadline)
            {
                System.err.println("[Acceptor] FATAL: OmsApp not connected after 30s — " +
                    "ensure OmsMediaDriverMain and OmsApp are both running before starting the acceptor.");
                library.close();
                fixAggRunner.close();
                eventStreamSub.close();
                verifyNewOrderSub.close();
                verifyEventSub.close();
                sequencedCommandSub.close();
                ingressCommandPub.close();
                ingressCommandPub.close();
                omsAeron.close();
                engine.close();
                artioDriver.close();
                return;
            }
            library.poll(10);
            Thread.sleep(10);
        }
        System.out.println("[Acceptor] OmsApp pipeline connected — ready to accept FIX orders.");

        // Shutdown: library → agents → omsAeron → engine → artioDriver.
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            System.out.println("[Acceptor] Shutting down — Artio will send Logout to all sessions.");
            library.close();
            fixAggRunner.close();
            eventStreamSub.close();
            verifyNewOrderSub.close();
            verifyEventSub.close();
            sequencedCommandSub.close();
            ingressCommandPub.close();
            ingressCommandPub.close();
            omsAeron.close();
            engine.close();
            artioDriver.close();
        }));

        System.out.println("[Acceptor] Listening on port " + FIX_PORT + ". Press Ctrl-C to stop.");

        while (!Thread.currentThread().isInterrupted())
        {
            library.poll(10);

            // M7: poll EVENT_STREAM for OrderAccepted/OrderRejected → send FIX ExecutionReport.
            // Polled on this thread (same as library.poll) so session.trySend() is thread-safe.
            eventStreamSub.poll(execReportBridge, 10);

//            // Verify 1: PlaceOrderCommand on COMMAND_STREAM(1) — confirms FIX→domain translation.
//            verifyNewOrderSub.poll((buf, off, len, hdr) ->
//            {
//                verifyHeaderDecoder.wrap(buf, off);
//                if (verifyHeaderDecoder.templateId() == NewOrderCommandDecoder.TEMPLATE_ID)
//                {
//                    verifyNewOrderDecoder.wrapAndApplyHeader(buf, off, verifyHeaderDecoder);
//                    System.out.printf("[M7-Verify] PlaceOrderCommand on COMMAND_STREAM: orderId=%d symbol=%s side=%s seq=%d%n",
//                        verifyPlaceDecoder.orderId(),
//                        verifyPlaceDecoder.symbol(),
//                        verifyPlaceDecoder.side(),
//                        verifyPlaceDecoder.sequenceNumber());
//                }
//            }, 10);
//
//            // Verify 2: OrderAcceptedEvent(100) / OrderRejectedEvent(101) on EVENT_STREAM(2)
//            // — confirms OmsApp's OrderAggregateAgent processed the PlaceOrderCommand.
//            verifyEventSub.poll((buf, off, len, hdr) ->
//            {
//                verifyHeaderDecoder.wrap(buf, off);
//                final int tid = verifyHeaderDecoder.templateId();
//                if (tid == 100) {
//                    System.out.println("[M7-Verify] OrderAcceptedEvent on EVENT_STREAM — order live!");
//                } else if (tid == 101) {
//                    System.out.println("[M7-Verify] OrderRejectedEvent on EVENT_STREAM — order rejected.");
//                }
//            }, 10);
        }
    }
}
