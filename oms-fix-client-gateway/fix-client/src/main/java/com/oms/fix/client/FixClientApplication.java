package com.oms.fix.client;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.nio.file.Paths;

@SpringBootApplication
public class FixClientApplication
{
    private static final String AERON_DIR    = "./aeron-fix-client";
    private static final String ARCHIVE_DIR  = "./fix-client-archive";
    private static final String ARCHIVE_CTRL = "aeron:udp?endpoint=localhost:8020";
    // Replication channel is unused in POC but required by Aeron 1.44.x Archive.Context.
    // TODO(POC): configure replication for HA setups.
    private static final String ARCHIVE_REPL = "aeron:udp?endpoint=localhost:0";

    public static void main(final String[] args)
    {
        // aeron.dir must be set before Artio or ArchivingMediaDriver are initialised.
        System.setProperty("aeron.dir",
            Paths.get(AERON_DIR).toAbsolutePath().normalize().toString());
        SpringApplication.run(FixClientApplication.class, args);
    }

    /**
     * Standalone ArchivingMediaDriver for the FIX client process.
     * Spring calls {@code close()} on this bean during context shutdown,
     * which happens AFTER {@link FixInitiatorService#stop()} (SmartLifecycle).
     */
    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver artioDriver()
    {
        final String aeronDir = System.getProperty("aeron.dir");
        return ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true)
                // DEDICATED so the conductor thread is never starved — prevents DriverTimeoutException
                // on a loaded multi-JVM system. TODO(POC): SharingMode.SHARED is fine for single process.
                .threadingMode(ThreadingMode.DEDICATED),
            new Archive.Context()
                .archiveDir(new File(ARCHIVE_DIR))
                .deleteArchiveOnStart(true)
                .controlChannel(ARCHIVE_CTRL)
                .replicationChannel(ARCHIVE_REPL)
                .recordingEventsEnabled(false)
                .aeronDirectoryName(aeronDir));
    }
}
