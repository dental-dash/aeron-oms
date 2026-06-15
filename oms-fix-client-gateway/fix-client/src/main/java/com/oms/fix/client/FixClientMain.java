package com.oms.fix.client;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Original bare Artio initiator (M2/M3). Superseded by {@link FixClientApplication}
 * + {@link FixInitiatorService} in Milestone 9. Kept for reference; not executed.
 *
 * // TODO(POC): Remove once M9 is fully validated.
 */
public class FixClientMain
{
    private static final String LIBRARY_CHANNEL = "aeron:ipc";
    private static final String ACCEPTOR_HOST   = "localhost";
    private static final int    ACCEPTOR_PORT   = 9880;
    private static final String LOG_DIR         = "./fix-client-logs";
    private static final String ARCHIVE_DIR     = "./fix-client-archive";
    // Dedicated Aeron dir — prevents client dirDeleteOnStart from wiping the acceptor's live dir.
    private static final String AERON_DIR       = "./aeron-fix-client";

    // Fixed UDP ports for archive control — avoids any IPC stream-ID collision.
    // Response on a fixed port (8021) prevents the ephemeral-port hang seen with :0.
    // Does not conflict with OmsApp's archive (8010) or the FIX acceptor's archive (8030).
    private static final String ARCHIVE_CONTROL_CHANNEL     = "aeron:udp?endpoint=localhost:8020";
    private static final String ARCHIVE_RESPONSE_CHANNEL    = "aeron:udp?endpoint=localhost:8021";
    private static final String ARCHIVE_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    static final String SENDER_COMP_ID = "CLIENT";
    static final String TARGET_COMP_ID = "ACCEPTOR";

    public static void main(final String[] args) throws Exception
    {
        // Resolve the Aeron dir to a canonical absolute path ONCE so that all components
        // (driver, archive, engine) use identical string paths and share the same CnC file.
        final String aeronDirAbsolute = Paths.get(AERON_DIR).toAbsolutePath().normalize().toString();
        // Force Artio's internal Aeron.connect() calls to use this same dir.
        System.setProperty("aeron.dir", aeronDirAbsolute);

        // Step 1: Launch ArchivingMediaDriver for this process.
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirAbsolute)
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED);

        final Archive.Context archiveCtx = new Archive.Context()
                .archiveDir(new File(ARCHIVE_DIR))
                .deleteArchiveOnStart(true)
                .controlChannel(ARCHIVE_CONTROL_CHANNEL)
                .replicationChannel(ARCHIVE_REPLICATION_CHANNEL)
                .recordingEventsEnabled(false)
                .aeronDirectoryName(aeronDirAbsolute);  // match the driver dir explicitly

        final ArchivingMediaDriver archivingMediaDriver =
                ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        // Step 2: Configure FixEngine — initiator has no bindTo().
        final EngineConfiguration engineCfg = new EngineConfiguration()
                .libraryAeronChannel(LIBRARY_CHANNEL)
                .logFileDir(LOG_DIR)
                .deleteLogFileDirOnStart(true)
                .scheduler(new LowResourceEngineScheduler());

        engineCfg.aeronArchiveContext()
                .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)   // UDP 8020
                .controlResponseChannel(ARCHIVE_RESPONSE_CHANNEL) // UDP 8021 — fixed port, not ephemeral
                .aeronDirectoryName(aeronDirAbsolute);

        // Step 3: Launch engine then library.
        final FixEngine engine = FixEngine.launch(engineCfg);

        // M9: FixClientSessionHandler is now a Spring component — not usable here.
        // Use FixClientApplication instead of this class.
        final LibraryConfiguration libraryCfg = new LibraryConfiguration()
                .sessionAcquireHandler(null)
                .sessionExistsHandler(
                        (library, surrogateId, localCompId, localSubId, localLocationId,
                         remoteCompId, remoteSubId, remoteLocationId, logonSeqNum, seqIndex) ->
                                System.out.printf("[Client] Existing session: local=%s remote=%s%n",
                                        localCompId, remoteCompId))
                .libraryAeronChannels(Collections.singletonList(LIBRARY_CHANNEL));

        final FixLibrary library = FixLibrary.connect(libraryCfg);

        // Warm-up: poll ~500 ms to let the engine's Framer fully register this library
        // before sending an initiate request. Without this, initiate() can immediately
        // error with "Not connected to the Gateway" on a fast startup path.
        for (int i = 0; i < 50; i++)
        {
            library.poll(10);
            Thread.sleep(10);
        }

        // Step 4: Initiate the FIX session — non-blocking; poll until complete.
        final SessionConfiguration sessionCfg = SessionConfiguration.builder()
                .address(ACCEPTOR_HOST, ACCEPTOR_PORT)
                .senderCompId(SENDER_COMP_ID)
                .targetCompId(TARGET_COMP_ID)
                // ResetSeqNumFlag (tag 141=Y): tells the acceptor to reset its expected inbound
                // sequence number on every Logon. Required when the client process restarts and
                // its log dir is deleted (deleteLogFileDirOnStart=true resets client seqnums to 1,
                // but the acceptor still remembers the previous session's last seqnum).
                // TODO(POC): remove if persistent sequence tracking is needed across client restarts.
                .resetSeqNum(true)
                .build();

        System.out.printf("[Client] Initiating FIX session → %s:%d (sender=%s target=%s)%n",
                ACCEPTOR_HOST, ACCEPTOR_PORT, SENDER_COMP_ID, TARGET_COMP_ID);

        final Reply<Session> sessionReply = library.initiate(sessionCfg);
        while (sessionReply.isExecuting())
        {
            library.poll(10);
            Thread.sleep(1);
        }

        if (sessionReply.hasCompleted())
        {
            final Session session = sessionReply.resultIfPresent();
            System.out.printf("[Client] Logon complete: sessionId=%d state=%s%n",
                    session != null ? session.id() : -1L,
                    session != null ? session.state() : "null");
        }
        else
        {
            System.err.printf("[Client] Session initiation failed: state=%s error=%s%n",
                    sessionReply.state(), sessionReply.error());
            library.close();
            engine.close();
            archivingMediaDriver.close();
            return;
        }

        // Shutdown hook: close in reverse order — library → engine → archivingMediaDriver.
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            System.out.println("[Client] Shutting down — sending Logout.");
            library.close();
            engine.close();
            archivingMediaDriver.close();
        }));

        System.out.println("[Client] Session active. Press Ctrl-C to disconnect.");

        // Keep polling: drives heartbeat generation and inbound message processing.
        while (!Thread.currentThread().isInterrupted())
        {
            library.poll(10);
            Thread.sleep(1);
        }
    }
}