# Aeron Component Reference — OMS POC

This series of documents describes the key open-source Aeron components used in this
Order Management System POC.

---

## Document Index

| # | File | What it covers |
|---|------|----------------|
| 01 | [Aeron Core](01-aeron-core.md) | Publishers, Subscribers, Drivers, channels & streams |
| 02 | [Agrona](02-agrona.md) | The low-level concurrency and data-structure library underpinning Aeron |
| 03 | [SBE](03-sbe.md) | Simple Binary Encoding — zero-copy, allocation-free message serialisation |
| 04 | [Aeron Archive](04-aeron-archive.md) | Persistent recording and replay of Aeron streams |
| 05 | [Aeron Cluster](05-aeron-cluster.md) | Raft-based replicated state machine (context only — not in POC scope) |

---

## Mental Model

```
┌─────────────────────────────────────────────────┐
│                  Your Application               │
│                                                 │
│  SBE-encoded messages                           │
│       │                                         │
│  Aeron Publication  ──►  Aeron Subscription     │
│       │                        │                │
└───────┼────────────────────────┼────────────────┘
        │                        │
┌───────▼────────────────────────▼────────────────┐
│            Media Driver (separate process        │
│            or embedded)                          │
│                                                  │
│   IPC shared memory  /  UDP unicast/multicast    │
│                                                  │
│   Aeron Archive (optional recording layer)       │
└──────────────────────────────────────────────────┘

Agrona underpins everything above (ring buffers, counters, off-heap buffers)
```

---

## Key Design Principles to Keep in Mind

**Zero allocation on the hot path.** Aeron is designed so that once your publications
and subscriptions are set up, no heap objects are created during normal message flow.
SBE is chosen precisely because it encodes/decodes directly onto `DirectBuffer`s without
creating intermediate objects.

**Separate Media Driver.** The `MediaDriver` is the process (or thread) that owns the
network/IPC transport. Your application talks to it via shared memory ring buffers
(managed by Agrona). This means your application logic is decoupled from OS socket
operations.

**Back-pressure is explicit.** `Publication.offer()` returns a status code, not an
exception. Your code must handle `NOT_CONNECTED`, `BACK_PRESSURED`, and `ADMIN_ACTION`
return values explicitly. This is intentional — exceptions on the hot path are
unacceptable in low-latency systems.

---

## Dependency Overview (Gradle)

```groovy
def aeronVersion = '1.44.0' // check https://github.com/real-logic/aeron/releases

dependencies {
    implementation "io.aeron:aeron-client:${aeronVersion}"
    implementation "io.aeron:aeron-driver:${aeronVersion}"
    implementation "io.aeron:aeron-archive:${aeronVersion}"
    implementation "io.aeron:aeron-cluster:${aeronVersion}" // only if using Cluster
    implementation "org.agrona:agrona:1.22.0"              // often pulled transitively
    implementation "uk.co.real-logic:sbe-tool:1.30.0"      // SBE code generator
}
```

> All libraries are available on Maven Central under the `io.aeron` and
> `org.agrona` group IDs.
