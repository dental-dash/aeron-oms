# 02 — Agrona

## What Is Agrona?

Agrona is a low-level Java library of high-performance data structures, buffers, and
concurrency utilities. It is developed by Real Logic and is the foundational layer
beneath Aeron, SBE, and Artio. You will interact with Agrona directly whenever you
are reading from or writing to Aeron messages, because all Aeron I/O is done via
Agrona's `DirectBuffer` abstractions.

GitHub: https://github.com/real-logic/agrona

---

## Why It Exists

Standard Java I/O (`ByteBuffer`, `InputStream`, `DataOutputStream`) involves heap
allocations, JVM boundary checks that defeat JIT optimisation, and unnecessary copying.
Agrona provides off-heap (`sun.misc.Unsafe`-backed) buffer abstractions that the JIT
can optimise aggressively, enabling direct memory reads and writes with minimal overhead.

---

## Core Types You Will Use

### `DirectBuffer` / `MutableDirectBuffer`

These are the central buffer interfaces in Agrona. `DirectBuffer` is read-only;
`MutableDirectBuffer` adds write operations.

| Interface | Use |
|-----------|-----|
| `DirectBuffer` | Read-only view of memory (e.g., inside a fragment handler) |
| `MutableDirectBuffer` | Read-write buffer for encoding outbound messages |

Key methods mirror `ByteBuffer` but without object creation:

```java
// Reading
int    value = buffer.getInt(offset);
long   value = buffer.getLong(offset);
byte   value = buffer.getByte(offset);

// Writing (MutableDirectBuffer)
buffer.putInt(offset, value);
buffer.putLong(offset, value);
buffer.putBytes(offset, srcBuffer, srcOffset, length);
```

> All methods take an explicit byte `offset` rather than maintaining a position cursor.
> This is intentional — it eliminates state management overhead and is safer for
> concurrent readers on the same buffer.

---

### `UnsafeBuffer`

`UnsafeBuffer` is the primary concrete implementation you will use for encoding
outbound messages. It wraps either a direct `ByteBuffer` or a raw memory address.

```java
// Allocate a reusable buffer for encoding — do this ONCE at startup, not per message
private final UnsafeBuffer encodeBuffer =
    new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

// Encode into it (SBE will do this for you — shown here for illustration)
encodeBuffer.putInt(0, orderId);
encodeBuffer.putDouble(4, price);

// Offer to Aeron
publication.offer(encodeBuffer, 0, encodedLength);
```

> **Never** allocate `UnsafeBuffer` per message. Allocate once and reuse.

---

### `ExpandableArrayBuffer` and `ExpandableDirectByteBuffer`

Heap-backed and direct buffers that grow automatically when you write past their
current capacity. Useful for variable-length messages in test/tooling code, but
avoid in hot-path production code because resizing allocates.

```java
ExpandableArrayBuffer buf = new ExpandableArrayBuffer(256);
buf.putStringUtf8(0, "hello"); // grows if needed
```

---

### `RingBuffer` and `ManyToOneRingBuffer`

Agrona's `ManyToOneRingBuffer` is a lock-free, bounded MPSC (multiple-producer,
single-consumer) ring buffer. This is exactly what Aeron uses internally to
communicate between your application and the Media Driver. You may also use it
directly within your application for passing messages between components.

```java
// Create — size must be power of 2 + RingBufferDescriptor.TRAILER_LENGTH
int bufferSize = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
UnsafeBuffer internalBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bufferSize));
ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(internalBuffer);

// Producer (any thread)
ringBuffer.write(MSG_TYPE_ORDER, srcBuffer, 0, encodedLength);

// Consumer (single thread)
ringBuffer.read((msgTypeId, buffer, index, length) -> {
    // process message
});
```

---

### `AtomicCounter` and `CountersReader`

Agrona provides off-heap atomic counters used heavily by Aeron for metrics and
liveness signals (e.g., subscriber heartbeats, publisher position). You can read
Aeron's internal counters for monitoring:

```java
CountersReader counters = aeron.countersReader();
counters.forEach((counterId, typeId, keyBuffer, label) -> {
    System.out.println(label + " = " + counters.getCounterValue(counterId));
});
```

---

### `IdleStrategy` (defined in Agrona)

Although used primarily via Aeron, `IdleStrategy` is an Agrona interface. See
[01-aeron-core.md](01-aeron-core.md) for the full comparison table. Implement it
yourself if you want custom back-off behaviour:

```java
public class CustomIdleStrategy implements IdleStrategy {
    public void idle(int workCount) { /* your logic */ }
    public void idle()              { /* called when workCount == 0 */ }
    public void reset()             { /* called before a new idle cycle */ }
}
```

---

### `CloseHelper`

A utility for safely closing `AutoCloseable` resources without throwing, useful when
you have a chain of resources to tear down:

```java
CloseHelper.quietClose(subscription);
CloseHelper.quietClose(publication);
CloseHelper.quietClose(aeron);
CloseHelper.quietClose(driver);

// Or close multiple, swallowing and logging exceptions
CloseHelper.quietCloseAll(subscription, publication, aeron, driver);
```

---

### `SystemEpochClock` and `SystemNanoClock`

Agrona provides clock abstractions that make testing easier by allowing clock
injection. Use `NanoClock` for latency measurements:

```java
NanoClock clock = new SystemNanoClock();
long start = clock.nanoTime();
// ... do work ...
long latencyNs = clock.nanoTime() - start;
```

---

## Agrona and the OMS

In the OMS POC, Agrona surfaces at three points:

1. **Message encoding** — SBE generates code that writes directly into an
   `UnsafeBuffer`. You allocate the buffer once and reuse it for every order message.

2. **Fragment handler** — When Aeron delivers a message to your subscriber callback,
   the `buffer` argument is an Agrona `DirectBuffer`. Read your SBE fields from it
   at the given `offset` without copying.

3. **Idle strategies** — Your polling loops use Agrona's `IdleStrategy` implementations
   to control CPU consumption between bursts of order flow.

---

## Key Rule

> Agrona's contract: **you own your buffer lifecycle**. Aeron will never hold a
> reference to a buffer you passed to `offer()` after the call returns. Inside a
> fragment handler, the `buffer` reference is only valid for the duration of the
> callback — do not store it.
