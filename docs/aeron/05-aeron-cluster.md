# 05 — Aeron Cluster

## What Is Aeron Cluster?

Aeron Cluster is a Raft-based **replicated state machine** framework built on top of
Aeron and Aeron Archive. It provides fault-tolerant, linearisable consensus across a
cluster of nodes — the same conceptual role as Apache ZooKeeper, etcd, or a Raft
library, but designed for microsecond-latency workloads.

GitHub: https://github.com/real-logic/aeron (Cluster is part of the main repo)
Artifact: `io.aeron:aeron-cluster`

---

> **POC Scope Note:** Aeron Cluster is **not in scope** for this OMS POC. It is
> documented here for situational awareness — so you understand where Cluster fits in
> the Aeron ecosystem and can make an informed decision if the POC evolves toward
> a production-grade, highly-available system.

---

## How It Relates to the Rest of Aeron

```
Aeron Cluster
    └── uses Aeron Archive  (for replicated log persistence)
            └── uses Aeron Core  (transport)
                    └── uses Agrona  (buffers, data structures)
```

Cluster is the highest-level component. It co-opts Archive to durably replicate the
Raft log across all cluster members before committing a command.

---

## Core Concepts

### Clustered Service

Your business logic implements the `ClusteredService` interface. Cluster calls your
service's `onSessionMessage()` method for every committed command, in the same order,
on every node. This is what makes the state machine replicated — all nodes execute the
same commands in the same order.

```java
public class OmsClusteredService implements ClusteredService {

    @Override
    public void onSessionMessage(
            ClientSession session,
            long timestamp,
            DirectBuffer buffer, int offset, int length,
            Header header) {

        // Decode the SBE message and update OMS state
        // This is called on ALL cluster nodes, so state stays in sync
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        // Load state from snapshot if restarting
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        // Persist current OMS state so nodes can restart without full log replay
    }

    // ... other lifecycle methods ...
}
```

### Leader and Followers

At any time one node is the **leader** and handles client ingress. Followers replicate
the log and are ready to take over if the leader fails. Client sessions are
transparently redirected to the new leader after an election.

### Ingress and Egress

Clients connect to the cluster via a dedicated **ingress channel**. The cluster
responds via an **egress channel**. This is conceptually equivalent to publication/
subscription in plain Aeron, but routed through the consensus layer.

### Snapshots

To avoid replaying the entire log on restart, the cluster periodically takes a
**snapshot** — a serialised copy of your service's state. On restart, the node loads
the latest snapshot and replays only the log entries since that snapshot.

---

## When Would You Add Cluster to the OMS?

| Requirement | Cluster needed? |
|-------------|----------------|
| POC / single-node demonstration | No |
| Fault tolerance (survive node failure without data loss) | **Yes** |
| Zero-downtime rolling upgrades | **Yes** |
| Regulatory requirement for HA order processing | **Yes** |
| Multi-datacenter replication | **Yes** (with careful latency budgeting) |

---

## What Changes When You Move From POC to Clustered?

The good news is that your SBE message schemas stay the same. The main changes are:

**Publications become cluster ingress.** Instead of `aeron.addPublication(...)`,
clients send messages via `AeronCluster.sendKeepAlive()` / `offer()` on the cluster
client's ingress publication.

**Subscriptions become the cluster egress.** Your execution reports are sent back via
`ClientSession.offer()` inside `onSessionMessage()`.

**State must be snapshot-able.** Any in-memory state your OMS holds (open orders, risk
positions) must be serialisable to a snapshot publication and reloadable from it.

**The service becomes deterministic.** Side effects (external calls, `System.currentTimeMillis()`)
must be driven from cluster-provided timestamps, not wall-clock time, so all nodes
produce identical state.

---

## Further Reading

- [Aeron Cluster Tutorial](https://github.com/real-logic/aeron/wiki/Cluster-Tutorial)
- [Cluster sample code](https://github.com/real-logic/aeron/tree/master/aeron-samples/src/main/java/io/aeron/samples/cluster)
- Martin Thompson's talks on Aeron Cluster at QCon and Strange Loop (search YouTube)

---

## Summary

Aeron Cluster is the natural evolution path for this OMS once the POC proves the
concept. For now, understanding that it exists and that your SBE schemas and Aeron
transport knowledge carry forward directly means the POC is not throwaway work — it
is the foundation layer of a production system.
