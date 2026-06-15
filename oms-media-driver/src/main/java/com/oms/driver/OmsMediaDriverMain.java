package com.oms.driver;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SigIntBarrier;

import java.io.File;

/**
 * Standalone Aeron MediaDriver + Archive process shared by all OMS components.
 *
 * <p>Start this process first. All other processes (OmsApp, FixAcceptorMain, FixClientMain)
 * connect to the shared Aeron dir at {@value #AERON_DIR} instead of embedding their own driver.
 *
 * <p>Benefits vs embedded drivers:
 * <ul>
 *   <li>Single archive instance — all recordings in one place, unified replay
 *   <li>No port sprawl — single control port 8010/8011
 *   <li>DEDICATED threading — no SHARED-mode thread starvation across processes
 *   <li>Driver survives individual process restarts
 * </ul>
 *
 * // TODO(POC): add Archive replication to a secondary node for HA
 */
public class OmsMediaDriverMain {

    static final String AERON_DIR           = "/tmp/aeron-oms";
    static final String ARCHIVE_DIR         = "./oms-archive";
    static final String CONTROL_CHANNEL     = "aeron:udp?endpoint=localhost:8010";
    static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8011";

    public static void main(String[] args) {
        // Set system property so any component that calls Aeron.connect() without an explicit
        // aeronDirectoryName() will automatically find the shared driver.
        System.setProperty("aeron.dir", AERON_DIR);

        final ArchivingMediaDriver driver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(AERON_DIR)
                .dirDeleteOnStart(true)
                // spiesSimulateConnection(true): Archive spy subscriptions count as connected
                // subscribers. REQUIRED — without this, IPC publications block at NOT_CONNECTED
                // because the Archive spy is the only subscriber on recording-enabled streams.
                .spiesSimulateConnection(true)
                // DEDICATED gives each internal thread (conductor, sender, receiver) its own
                // dedicated OS thread. Avoids the SHARED-mode starvation seen when multiple
                // processes compete for the same shared driver thread.
                // TODO(POC): tune thread affinity for production
                .threadingMode(ThreadingMode.DEDICATED),
            new Archive.Context()
                .archiveDir(new File(ARCHIVE_DIR))
                // Preserve recordings across restarts — essential for replay-based recovery.
                // TODO(POC): set deleteArchiveOnStart(true) only in CI/test environments
                .deleteArchiveOnStart(false)
                .controlChannel(CONTROL_CHANNEL)
                // replicationChannel is mandatory in Aeron 1.44.x even if HA replication
                // is not used. Points at a local endpoint that is never actually connected to.
                // TODO(POC): wire real secondary Archive node for production HA
                .replicationChannel(REPLICATION_CHANNEL)
                .recordingEventsEnabled(false)  // TODO(POC): enable for production monitoring
                .aeronDirectoryName(AERON_DIR)
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MediaDriver] Shutting down.");
            driver.close();
        }, "oms-media-driver-shutdown"));

        System.out.printf("[MediaDriver] Running — aeronDir=%s  archive=%s  ctrl=%s%n",
            AERON_DIR, ARCHIVE_DIR, CONTROL_CHANNEL);

        // Block main thread until Ctrl-C. SigIntBarrier installs a SIGINT handler that
        // unblocks await(), then the shutdown hook above closes the driver cleanly.
        new SigIntBarrier().await();
    }
}
