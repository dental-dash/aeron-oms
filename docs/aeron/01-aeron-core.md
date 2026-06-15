# 01 — Aeron Core

## What Is Aeron?

Aeron is an open-source, high-performance messaging library developed by Real Logic.
It is designed for low-latency, high-throughput communication between processes on the
same machine (IPC) or across a network (UDP unicast/multicast). It is used in
financial exchanges, trading systems, and real-time infrastructure where microsecond
latency matters.

GitHub: https://github.com/real-logic/aeron

---

## Core Concepts

### Media Driver

The `MediaDriver` is the heart of Aeron. It is a standalone process (or embedded thread)
responsible for all I/O — reading from and writing to network sockets or shared memory.
Your application never touches sockets directly; it communicates with the driver via
shared memory ring buffers (provided by Agrona).

**Two deployment modes:**

| Mode | Class | When to use |
|------|-------|-------------|
| Embedded | `MediaDriver.launch()` | POC, testing, single-JVM setups |
| External | `aeronmd` binary or separate JVM | Production; isolates I/O from app GC pauses |

```java
// Embedded driver — simplest POC setup
MediaDriver.Context ctx = new MediaDriver.Context()
    .dirDeleteOnStart(true)
    .threadingMode(ThreadingMode.SHARED); // single thread; use DEDICATED for prod

try (MediaDriver driver = MediaDriver.launch(ctx)) {
    // your app runs here
}
```

> `ThreadingMode.SHARED` runs conductor, sender, and receiver on one thread —
> fine for a POC. Use `ThreadingMode.DEDICATED` in production to pin each to a
> separate CPU core.

---

### Aeron Client

The `Aeron` object is your application's handle to the driver. It is thread-safe for
adding/removing publications and subscriptions, but `Publication` and `Subscription`
objects themselves should be used from a single thread.

```java
Aeron.Context aeronCtx = new Aeron.Context()
    .aeronDirectoryName(driver.aeronDirectoryName());

try (Aeron aeron = Aeron.connect(aeronCtx)) {
    // create publications and subscriptions from here
}
```

---

### Channels and Streams

Every Aeron endpoint is identified by a **channel** (the transport) and a **stream ID**
(a logical multiplexer within that channel). Think of a channel as a socket address and
a stream ID as a topic.

**Common channel URIs:**

| URI | Transport |
|-----|-----------|
| `aeron:ipc` | Shared memory IPC — same machine, lowest latency |
| `aeron:udp?endpoint=localhost:20121` | UDP unicast |
| `aeron:udp?control-mode=dynamic\|control=localhost:20121` | UDP dynamic MDC |
| `aeron:udp?endpoint=224.0.1.1:20121` | UDP multicast |

Stream IDs are arbitrary positive integers. By convention, define them as named
constants in your codebase:

```java
public final class StreamIds {
    public static final int NEW_ORDER   = 10;
    public static final int CANCEL      = 11;
    public static final int EXEC_REPORT = 20;
}
```

---

### Publication

A `Publication` sends messages. The key method is `offer()`, which attempts to write
an encoded message into the underlying ring buffer.

```java
try (Publication pub = aeron.addPublication("aeron:ipc", StreamIds.NEW_ORDER)) {

    // Wait until a subscriber is connected
    while (!pub.isConnected()) {
        Thread.sleep(1);
    }

    UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
    // ... encode your message into buffer using SBE ...

    long result = pub.offer(buffer, 0, encodedLength);

    if (result < 0) {
        // Handle back-pressure / errors — see return codes below
    }
}
```

**`offer()` return codes — you must handle all of these:**

| Constant | Value | Meaning |
|----------|-------|---------|
| `Publication.NOT_CONNECTED` | -1 | No active subscriber yet |
| `Publication.BACK_PRESSURED` | -2 | Subscriber is too slow; ring buffer full |
| `Publication.ADMIN_ACTION` | -3 | Driver performing log rotation — retry |
| `Publication.CLOSED` | -4 | Publication has been closed |
| `Publication.MAX_POSITION_EXCEEDED` | -5 | Stream position limit reached |
| `>= 0` | position | Success; value is the new stream position |

---

### Subscription and Fragment Handlers

A `Subscription` receives messages. You poll it on a loop, providing a
`FragmentHandler` callback that is invoked for each message fragment received.

```java
FragmentHandler handler = (buffer, offset, length, header) -> {
    // Decode SBE message from buffer at offset/length
    // header contains stream position, session ID, flags, etc.
};

try (Subscription sub = aeron.addSubscription("aeron:ipc", StreamIds.NEW_ORDER)) {

    IdleStrategy idle = new BusySpinIdleStrategy(); // or SleepingIdleStrategy for POC

    while (running) {
        int fragmentsRead = sub.poll(handler, 10); // poll up to 10 fragments
        idle.idle(fragmentsRead);
    }
}
```

> **Fragment vs Message:** A large message may be split into multiple fragments by
> Aeron. Use a `FragmentAssembler` to transparently reassemble them before passing
> to your handler. For the OMS POC, SBE messages will be small enough that
> fragmentation is unlikely, but it is good practice to use `FragmentAssembler` anyway.

```java
FragmentAssembler assembler = new FragmentAssembler(
    (buffer, offset, length, header) -> {
        // guaranteed to receive complete messages here
    }
);
sub.poll(assembler, 10);
```

---

### Idle Strategies

An `IdleStrategy` controls what the polling thread does when there is no work.
Choosing the right one involves a latency vs. CPU trade-off:

| Strategy | Latency | CPU | Use case |
|----------|---------|-----|----------|
| `BusySpinIdleStrategy` | Lowest | 100% core | Production latency-critical |
| `YieldingIdleStrategy` | Low | High | Good default for POC |
| `SleepingIdleStrategy` | Higher | Low | Background threads, non-critical |
| `BackoffIdleStrategy` | Adaptive | Adaptive | General purpose |

For the POC, `YieldingIdleStrategy` is a reasonable default. Pin CPU cores and use
`BusySpinIdleStrategy` when you need to measure real latency.

---

## Lifecycle — Putting It Together

```java
// Canonical POC setup
try (
    MediaDriver      driver = MediaDriver.launch(new MediaDriver.Context().dirDeleteOnStart(true));
    Aeron            aeron  = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    Publication      pub    = aeron.addPublication("aeron:ipc", StreamIds.NEW_ORDER);
    Subscription     sub    = aeron.addSubscription("aeron:ipc", StreamIds.NEW_ORDER)
) {
    // publish and consume messages
}
// All resources closed in reverse order via try-with-resources
```

All Aeron objects implement `AutoCloseable`. Always use try-with-resources or
`CloseHelper.quietClose()` to avoid resource leaks.

---

## Common Mistakes for Java Developers New to Aeron

**Throwing exceptions on `offer()` failures.** Don't. Handle the negative return codes
with a retry loop and an idle strategy.

**Allocating objects in the fragment handler.** The whole point of Aeron + SBE is
zero-allocation on the hot path. Read directly from the `DirectBuffer` in your handler.

**Sharing a `Publication` across threads.** Publications are not thread-safe for
`offer()`. Use one publication per thread, or add explicit synchronisation (at the
cost of latency).

**Forgetting `dirDeleteOnStart(true)`.** Without this, stale Aeron directory state
from a previous crashed run will cause the driver to fail to start.
