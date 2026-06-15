# 04 — Aeron Archive

## What Is Aeron Archive?

Aeron Archive is an extension to Aeron that adds **persistent recording and replay**
of Aeron streams. It acts as a recording service layered on top of a standard Media
Driver, transparently storing every message published on a channel/stream combination
to local storage. Any recording can later be replayed as a standard Aeron subscription.

GitHub: https://github.com/real-logic/aeron (Archive is part of the main repo)
Artifact: `io.aeron:aeron-archive`

---

## Why Use It in the OMS?

| Capability | OMS Use Case |
|------------|--------------|
| **Crash recovery** | Replay unprocessed orders after a component restart |
| **Audit trail** | Immutable record of every order and execution report |
| **Regression testing** | Replay a production order sequence through a new version of the matching engine |
| **Slow consumer catch-up** | A risk monitor that restarts can replay from its last known position |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Your Application                                        │
│                                                          │
│  AeronArchive client  ──────────────────────────────┐   │
│  (start/stop recording, replay requests)            │   │
└─────────────────────────────────────────────────────┼───┘
                                                      │
┌─────────────────────────────────────────────────────▼───┐
│  Archive Service (embedded ArchivingMediaDriver          │
│  or separate process)                                    │
│                                                          │
│  ┌─────────────────┐     ┌──────────────────────────┐   │
│  │  Media Driver   │     │  Archive Catalog +        │   │
│  │  (IPC / UDP)    │     │  Segment Files on disk   │   │
│  └─────────────────┘     └──────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

The `ArchivingMediaDriver` is a `MediaDriver` with the Archive co-located. For the
POC this is the simplest setup. In production, the Archive runs as a separate process
to isolate disk I/O from application threads.

---

## Key Concepts

### Recording

A **recording** is a persisted copy of a stream on a specific channel, identified by a
`recordingId` (a monotonically increasing `long`). The Archive stores recordings as
fixed-size segment files (default 128 MB each) in the `archiveDir` you configure.

### Catalog

The **catalog** is a small index file (`archive.catalog`) maintained by the Archive.
It maps `recordingId` to metadata: channel, stream ID, start/stop positions, and file
locations. The `AeronArchive` client queries the catalog via the control channel.

### Control Channel and Response Channel

The `AeronArchive` client communicates with the Archive service over two dedicated
Aeron streams:

- **Control channel** — your app sends commands (start recording, stop, list, replay)
- **Response channel** — the Archive sends back confirmations and descriptors

These are handled transparently by the `AeronArchive` API; you rarely deal with them
directly unless customising the channel URIs.

---

## Setup — Embedded `ArchivingMediaDriver`

```java
// POC setup: Archive embedded alongside the Media Driver
String archiveDir = "/tmp/aeron-archive";

MediaDriver.Context driverCtx = new MediaDriver.Context()
    .dirDeleteOnStart(true)
    .threadingMode(ThreadingMode.SHARED);

Archive.Context archiveCtx = new Archive.Context()
    .archiveDir(new File(archiveDir))
    .deleteArchiveOnStart(true) // TODO(POC): remove in production
    .controlChannel("aeron:ipc")
    .controlStreamId(100)
    .recordingEventsEnabled(true);

try (
    ArchivingMediaDriver archivingDriver =
        ArchivingMediaDriver.launch(driverCtx, archiveCtx);

    Aeron aeron = Aeron.connect(new Aeron.Context()
        .aeronDirectoryName(archivingDriver.mediaDriver().aeronDirectoryName()));

    AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
        .aeron(aeron)
        .controlRequestChannel("aeron:ipc")
        .controlRequestStreamId(100))
) {
    // use archive here
}
```

---

## Starting a Recording

Tell the Archive to record all messages on a channel/stream. Any publisher that
subsequently publishes on that channel and stream will be recorded automatically.

```java
// Request the Archive to start recording new orders
long subscriptionId = archive.startRecording(
    "aeron:ipc",            // channel to record
    StreamIds.NEW_ORDER,    // stream ID
    SourceLocation.LOCAL    // LOCAL = same machine; REMOTE for external publishers
);

// subscriptionId identifies this recording instruction — store it if you
// need to stop recording later
```

> Recording begins the moment a publisher connects to that channel/stream. Past messages
> (before `startRecording` was called) are not captured.

---

## Listing Recordings

```java
// Find recordings for our order stream
archive.listRecordingsForUri(
    0,                          // from index
    100,                        // max records to return
    "aeron:ipc",
    StreamIds.NEW_ORDER,
    (controlSessionId, correlationId, recordingId,
     startTimestamp, stopTimestamp, startPosition, stopPosition,
     initialTermId, segmentFileLength, termBufferLength, mtuLength,
     sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {

        System.out.printf("recordingId=%d start=%d stop=%d%n",
            recordingId, startPosition, stopPosition);
    }
);
```

---

## Replaying a Recording

Replay creates a new Aeron publication on a channel of your choice. You subscribe to
that channel to receive the replayed messages exactly as they were originally recorded.

```java
long recordingId  = 0L; // from listRecordings
long replayFrom   = 0L; // start position (0 = from the beginning)
long replayLength = AeronArchive.NULL_LENGTH; // replay entire recording

// Ask the Archive to replay onto a new stream
long replaySessionId = archive.startReplay(
    recordingId,
    replayFrom,
    replayLength,
    "aeron:ipc",            // channel to replay onto
    StreamIds.REPLAY_ORDERS // stream ID for the replayed messages
);

// Subscribe to the replay stream
try (Subscription replaySub = aeron.addSubscription("aeron:ipc", StreamIds.REPLAY_ORDERS)) {

    // Wait for the replay publisher to connect
    while (!replaySub.isConnected()) {
        Thread.sleep(1);
    }

    IdleStrategy idle = new YieldingIdleStrategy();
    while (running) {
        int frags = replaySub.poll(myFragmentHandler, 10);
        idle.idle(frags);
        // The subscription will naturally complete when the recording ends
    }
}
```

> The replayed stream is indistinguishable from a live stream to your fragment handler.
> Your existing SBE decoding logic works without modification.

---

## Stopping a Recording

```java
archive.stopRecording(
    "aeron:ipc",
    StreamIds.NEW_ORDER
);

// Or stop by subscriptionId returned from startRecording
archive.stopRecording(subscriptionId);
```

---

## Truncating / Purging Recordings (POC Housekeeping)

In the POC it is useful to start fresh between runs:

```java
// List all recordings and purge them
// TODO(POC): In production, implement a retention policy instead
archive.purgeRecording(recordingId);
```

Or simply set `deleteArchiveOnStart(true)` in `Archive.Context` (as shown above).

---

## Recording in the OMS POC — Recommended Approach

Record two streams on startup before any publishers are created:

| Stream | Channel | `StreamIds` constant | Purpose |
|--------|---------|----------------------|---------|
| New Orders | `aeron:ipc` | `NEW_ORDER` | Full inbound order audit |
| Execution Reports | `aeron:ipc` | `EXEC_REPORT` | Full outbound execution audit |

On startup, query the catalog to find the last recorded position for each stream.
If the system crashed mid-session, replay from the last checkpointed position to
re-hydrate in-flight order state before accepting new orders.

```java
// Pseudocode: crash recovery on startup
long lastCheckpointPosition = readCheckpointFromStateStore();
if (lastCheckpointPosition > 0) {
    archive.startReplay(orderRecordingId, lastCheckpointPosition,
                        AeronArchive.NULL_LENGTH, "aeron:ipc", StreamIds.RECOVERY);
    // consume replay stream to restore state, then switch to live stream
}
```

---

## Common Mistakes

**Not waiting for the recording to be ready before publishing.** The Archive needs a
moment to set up its recording subscription after `startRecording()` returns. Use
`archive.findLastMatchingRecording(...)` or a `RecordingSignalAdapter` to confirm the
recording is active before your publisher connects.

**Using `deleteArchiveOnStart(true)` in production.** This is a POC shortcut only.
In production the archive is your audit log and crash-recovery source.

**Replaying to the same stream ID as the live stream.** Always use a dedicated
stream ID for replays to avoid mixing live and replayed messages in the same
subscription.
