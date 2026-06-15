# OMS PoC — Design & Architecture

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Architectural Principles](#core-architectural-principles)
3. [Component Breakdown](#component-breakdown)
    - [Sequencer](#1-sequencer)
    - [Order Ingress](#2-order-ingress)
    - [OrderAggregate](#3-orderaggregate)
    - [Event Handlers](#4-event-handlers)
    - [Read Models](#5-read-models)
    - [Aeron Archive](#6-aeron-archive)
4. [Message Flow — New Order End to End](#message-flow--new-order-end-to-end)
5. [SBE Message Schemas](#sbe-message-schemas)
6. [Gradle Project Structure](#gradle-project-structure)
7. [Key Design Details](#key-design-details)
    - [Channels & Stream IDs](#channels--stream-ids)
    - [OrderAggregate Threading Model](#orderaggregate-threading-model)
    - [Startup Replay Sequence](#startup-replay-sequence)

---

## System Overview

The OMS PoC is built around **two sequenced Aeron IPC streams** — a Command Stream and an Event Stream. Everything flows through these two streams. The Sequencer is the single point of ordering, meaning all participants see commands and events in the same deterministic sequence. This is the key invariant the whole design relies on.

```
                        ┌─────────────────────────────────────────┐
                        │           SEQUENCER                     │
                        │                                         │
  [Ingress] ──────────► │  Command Stream (IPC StreamId: 1)       │
                        │  Event Stream   (IPC StreamId: 2)       │
                        └──────────────┬──────────────────────────┘
                                       │ both streams fan out to all subscribers
                    ┌──────────────────┼───────────────────────┐
                    ▼                  ▼                        ▼
           [OrderAggregate]    [EventHandlers]          [Read Models]
```

### Architecture Patterns

- **Event-Sourcing** — every state change is an immutable event on the Event Stream; the aggregate applies state changes synchronously and immediately after publishing each event, ensuring consistency before the next command is processed. The Event Stream is used exclusively for startup state reconstruction via Archive replay.
- **CQRS** — the write side (Aggregate) and read side (Read Models) are completely separate; they share only the Event Stream.
- **Finite State Machine** — the `OrderAggregate` enforces valid order lifecycle transitions and rejects invalid commands.
- **Hexagonal Architecture** — domain logic in the aggregate and event handlers has no dependency on infrastructure; Aeron adapters plug in at the edges.
- **Sequenced Messaging** — both the Command and Event streams carry a monotonically increasing sequence number stamped by the Sequencer, giving a total ordering of all system activity.

---

## Core Architectural Principles

There are two canonical streams in the system. Both are written to **exclusively by the Sequencer** and read by all participants:

| Stream          | Aeron Channel | StreamId | Written by    | Read by                                      |
|-----------------|---------------|----------|---------------|----------------------------------------------|
| Command Stream  | `aeron:ipc`   | `1`      | Sequencer     | OrderAggregate                               |
| Event Stream    | `aeron:ipc`   | `2`      | Sequencer     | EventHandlers, ReadModels (+ OrderAggregate during startup replay only) |

There are also two **pre-sequencer ingress channels** that feed into the Sequencer. These are the only channels that external components write to:

| Channel              | Aeron Channel | StreamId | Written by                        |
|----------------------|---------------|----------|-----------------------------------|
| Command Ingress      | `aeron:ipc`   | `10`     | Order Ingress                     |
| Event Ingress        | `aeron:ipc`   | `11`     | OrderAggregate, EventHandlers     |

The Sequencer reads from StreamIds `10` and `11`, stamps a sequence number, and republishes onto StreamIds `1` and `2` respectively. This ensures every participant sees commands and events in a single globally consistent order.

---

## Component Breakdown

### 1. Sequencer

The Sequencer owns both canonical streams. It receives inbound messages from the pre-sequencer ingress channels, stamps each with a monotonically increasing sequence number on the `MessageHeader`, and re-publishes them to the appropriate canonical stream. It contains **no business logic** — it is purely a sequencing and fan-out mechanism.

The Aeron Archive attaches to both canonical streams so every message is durably recorded in sequence order.

**Responsibilities:**
- Read from Command Ingress (StreamId `10`) → stamp sequence → publish to Command Stream (StreamId `1`)
- Read from Event Ingress (StreamId `11`) → stamp sequence → publish to Event Stream (StreamId `2`)
- Both canonical streams are recorded by the Archive

### 2. Order Ingress

The Ingress is the external-facing entry point. It accepts orders from the outside world — REST API, FIX, or a test harness — and translates them into SBE-encoded commands published to the pre-sequencer Command Ingress channel. The Ingress has no knowledge of business rules; it is a pure protocol adapter.

**Responsibilities:**
- Expose REST endpoints (`POST /orders`, `DELETE /orders/{id}`, `PATCH /orders/{id}`)
- Translate inbound requests into `NewOrderCommand`, `CancelOrderCommand`, `AmendOrderCommand`
- SBE-encode commands and publish to Command Ingress (IPC StreamId `10`)
- Return a `correlationId` to the caller for tracking the async result

### 3. OrderAggregate

The aggregate is the heart of the system. It maintains authoritative in-memory order state and has a single live subscription and one publication. It does **not** subscribe to the Event Stream during normal operation — state is updated synchronously within command processing itself.

**Subscription — Command Stream (StreamId `1`)**

Reads `NewOrderCommand`, `CancelOrderCommand`, `AmendOrderCommand`. For each command it:
1. Reads current order state to validate the command
2. Applies business validation logic
3. Encodes the resulting domain event (`OrderAcceptedEvent`, `OrderRejectedEvent`, etc.) via SBE
4. Publishes the event to the pre-sequencer Event Ingress (StreamId `11`)
5. **Immediately applies the event to update its own in-memory state** before returning to poll the next command

Step 5 is critical for correctness. If the aggregate deferred state updates to a separate Event Stream subscription, a second command could arrive and be processed against stale state before the first event had been applied. By applying state changes synchronously and inline — immediately after a successful publish — the aggregate guarantees that its state is always consistent with the events it has emitted before it processes any further command.

```
for each Command on Command Stream:
    event = handleCommand(command, currentState)   // validate, produce event
    publish(event) to Event Ingress                // send to sequencer
    applyEvent(event, currentState)                // update state immediately
    // only now poll the next command
```

**Publication — Event Ingress (StreamId `11`)**

Publishes domain events resulting from command processing back to the Sequencer for sequencing and fan-out to all other subscribers (Event Handlers, Read Models).

**Order State Machine**

```
               ┌──────────┐
               │   NEW    │  ◄── NewOrderCommand received
               └────┬─────┘
                    │ OrderAcceptedEvent
                    ▼
               ┌──────────┐
               │   OPEN   │
               └────┬─────┘
          ┌─────────┼──────────┐
          │         │          │
          ▼         ▼          ▼
   ┌──────────┐  ┌──────┐  ┌──────────────┐
   │PARTIALLY │  │FILLED│  │  CANCELLED   │
   │  FILLED  │  └──────┘  └──────────────┘
   └──────────┘
          │
          ▼
      ┌──────┐
      │FILLED│
      └──────┘
```

Any command that would result in an invalid state transition (e.g., cancelling an already-filled order) is rejected with a `CancelRejectedEvent`.

**Startup / Replay**

On startup, before subscribing to the live Command Stream, the aggregate connects to the Aeron Archive and replays the entire Event Stream from position `0`. It applies every event via `applyEvent()` to rebuild in-memory state. The Event Stream subscription is used **exclusively** during this replay phase. Once replay is complete, the Event Stream subscription is closed and the aggregate switches to polling the live Command Stream.

```
Startup:        Archive replay (Event Stream) ──► applyEvent() × N ──► state current
                ──► close Event Stream subscription
                ──► begin polling Command Stream

Live operation: Command Stream ──► handleCommand()
                                ──► publish event to Event Ingress
                                ──► applyEvent() immediately (inline)
                                ──► poll next command
```

### 4. Event Handlers

Event Handlers are **stateless subscribers** to the Event Stream. They contain reactive business logic and respond to events by publishing new commands. Because they are stateless, they do not participate in Archive replay on startup — they simply begin listening from the live position.

Each Event Handler follows the same pattern:

```
Event Stream subscription ──► filter relevant events ──► publish Command(s) to Command Ingress
```

**PoC Event Handlers:**

| Handler                  | Listens For                | Publishes                          |
|--------------------------|----------------------------|------------------------------------|
| `FillSimulatorHandler`   | `OrderAcceptedEvent`       | Simulated `OrderFilledEvent` (via a FillCommand) |
| `RiskEventHandler`       | `OrderAcceptedEvent`       | `RiskCheckCommand`                 |
| `CancelOnRiskBreachHandler` | `RiskBreachEvent`       | `CancelOrderCommand`               |

The `FillSimulatorHandler` replaces the need for a real execution gateway in the PoC — it listens for accepted orders and deterministically or randomly generates fill events, allowing the full order lifecycle to be demonstrated without external connectivity.

### 5. Read Models

Read Models subscribe **only to the Event Stream** and build persistent or in-memory projections. They are completely decoupled from the command and aggregate path. Two implementations are provided for the PoC.

**DatabaseReadModel**

Subscribes to the Event Stream and upserts rows into an H2 (or Postgres) table. Provides the query-side for REST API order status lookups. Tracks its last processed sequence number so on restart it replays only from that checkpoint using the Aeron Archive.

**ViewServerReadModel**

Subscribes to the Event Stream and maintains an in-memory `ConcurrentHashMap<OrderId, OrderView>`. In production this would push diffs to a real ViewServer (e.g., Caplin, Perspective). For the PoC it exposes the map via a simple query interface consumed by the WebSocket push layer in `oms-api`.

Both Read Models implement the same `ReadModel` interface:

```kotlin
interface ReadModel {
    fun onEvent(event: DirectBuffer, offset: Int, length: Int, header: Header)
    fun lastProcessedSequence(): Long
}
```

### 6. Aeron Archive

The Archive runs as an embedded component in the PoC and records both the Command Stream and Event Stream in their entirety. It serves two purposes — durability and replay.

**Consumers of the Archive:**
- `OrderAggregate` on startup — replays the full Event Stream from position `0` to rebuild state
- `DatabaseReadModel` on restart — replays from its last checkpointed sequence number
- `ViewServerReadModel` on restart — replays from its last checkpointed sequence number
- Operational tooling — can audit the full command/event history at any time

---

## Message Flow — New Order End to End

```
 1. REST call hits Order Ingress
 2. Ingress encodes NewOrderCommand via SBE
    → publishes to Command Ingress (IPC StreamId:10)

 3. Sequencer reads StreamId:10
    → stamps sequence number
    → publishes to Command Stream (IPC StreamId:1)

 4. Archive records the command

 5. OrderAggregate reads Command Stream
    → validates NewOrderCommand (price > 0, qty > 0, account exists, no duplicate orderId)
    → encodes OrderAcceptedEvent (or OrderRejectedEvent) via SBE
    → publishes to Event Ingress (IPC StreamId:11)

 6. Sequencer reads StreamId:11
    → stamps sequence number
    → publishes to Event Stream (IPC StreamId:2)

 7. Archive records the event

 8. OrderAggregate applies OrderAcceptedEvent immediately to its own state (inline, synchronously)
    → order state = OPEN (before the next command is polled)

 9. FillSimulatorHandler reads Event Stream
    → sees OrderAcceptedEvent → publishes FillCommand to Command Ingress (StreamId:10)

10. Sequencer sequences FillCommand → Command Stream

11. (No aggregate handles FillCommand directly in PoC — FillSimulator publishes OrderFilledEvent
     directly to Event Ingress, or via a FillAggregate if extended)

12. OrderAggregate reads Event Stream
    → sees OrderFilledEvent → applyEvent() → order state = FILLED

13. ViewServerReadModel reads Event Stream
    → updates in-memory OrderView map

14. DatabaseReadModel reads Event Stream
    → upserts order row with status = FILLED

15. WebSocket layer queries ViewServer
    → pushes real-time update to connected clients
```

---

## SBE Message Schemas

All messages share a common 8-byte `MessageHeader` (standard SBE header: `blockLength`, `templateId`, `schemaId`, `version`). Every message body begins with a `sequenceNumber` (stamped by the Sequencer) and a `timestamp` (epoch microseconds).

**Template ID Ranges:**

| Range    | Category  |
|----------|-----------|
| 1 – 99   | Commands  |
| 100–199  | Events    |

**Schema file:** `oms-sbe/src/main/resources/oms-messages.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe"
                   package="com.oms.sbe"
                   id="1"
                   version="1"
                   semanticVersion="1.0"
                   description="OMS Commands and Events">

    <!-- ═══════════════════════════════════════════ -->
    <!-- SHARED TYPES                                -->
    <!-- ═══════════════════════════════════════════ -->

    <types>
        <type name="sequenceNumber"  primitiveType="int64"/>
        <type name="timestamp"       primitiveType="int64"/>  <!-- epoch micros -->
        <type name="orderId"         primitiveType="int64"/>
        <type name="accountId"       primitiveType="int64"/>
        <type name="price"           primitiveType="int64"/>  <!-- price × 10^8 fixed point -->
        <type name="quantity"        primitiveType="int64"/>  <!-- quantity × 10^8 fixed point -->
        <type name="correlationId"   primitiveType="int64"/>

        <enum name="Side" encodingType="uint8">
            <validValue name="BUY">0</validValue>
            <validValue name="SELL">1</validValue>
        </enum>

        <enum name="OrderType" encodingType="uint8">
            <validValue name="LIMIT">0</validValue>
            <validValue name="MARKET">1</validValue>
        </enum>

        <enum name="TimeInForce" encodingType="uint8">
            <validValue name="DAY">0</validValue>
            <validValue name="GTC">1</validValue>
            <validValue name="IOC">2</validValue>
            <validValue name="FOK">3</validValue>
        </enum>

        <enum name="RejectReason" encodingType="uint8">
            <validValue name="INVALID_PRICE">0</validValue>
            <validValue name="INVALID_QUANTITY">1</validValue>
            <validValue name="UNKNOWN_INSTRUMENT">2</validValue>
            <validValue name="UNKNOWN_ACCOUNT">3</validValue>
            <validValue name="RISK_BREACH">4</validValue>
            <validValue name="DUPLICATE_ORDER">5</validValue>
        </enum>

        <enum name="CancelReason" encodingType="uint8">
            <validValue name="CLIENT_REQUEST">0</validValue>
            <validValue name="RISK_BREACH">1</validValue>
            <validValue name="EXPIRED">2</validValue>
        </enum>

        <composite name="messageHeader"
                   description="Standard SBE message header">
            <type name="blockLength"  primitiveType="uint16"/>
            <type name="templateId"   primitiveType="uint16"/>
            <type name="schemaId"     primitiveType="uint16"/>
            <type name="version"      primitiveType="uint16"/>
        </composite>

        <type name="instrument" primitiveType="char" length="12"
              characterEncoding="US-ASCII"/>
    </types>

    <!-- ═══════════════════════════════════════════ -->
    <!-- COMMANDS  (templateId 1–99)                 -->
    <!-- ═══════════════════════════════════════════ -->

    <sbe:message name="NewOrderCommand" id="1"
                 description="Client submits a new order">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="instrument"      id="6"  type="instrument"/>
        <field name="side"            id="7"  type="Side"/>
        <field name="orderType"       id="8"  type="OrderType"/>
        <field name="timeInForce"     id="9"  type="TimeInForce"/>
        <field name="price"           id="10" type="price"/>
        <field name="quantity"        id="11" type="quantity"/>
    </sbe:message>

    <sbe:message name="CancelOrderCommand" id="2"
                 description="Client requests cancellation of an open order">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="cancelReason"    id="6"  type="CancelReason"/>
    </sbe:message>

    <sbe:message name="AmendOrderCommand" id="3"
                 description="Client amends price or quantity of an open order">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="newPrice"        id="6"  type="price"/>
        <field name="newQuantity"     id="7"  type="quantity"/>
    </sbe:message>

    <!-- ═══════════════════════════════════════════ -->
    <!-- EVENTS  (templateId 100–199)               -->
    <!-- ═══════════════════════════════════════════ -->

    <sbe:message name="OrderAcceptedEvent" id="100"
                 description="Order passed validation and is now open">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="instrument"      id="6"  type="instrument"/>
        <field name="side"            id="7"  type="Side"/>
        <field name="orderType"       id="8"  type="OrderType"/>
        <field name="timeInForce"     id="9"  type="TimeInForce"/>
        <field name="price"           id="10" type="price"/>
        <field name="quantity"        id="11" type="quantity"/>
    </sbe:message>

    <sbe:message name="OrderRejectedEvent" id="101"
                 description="Order failed validation or risk check">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="rejectReason"    id="6"  type="RejectReason"/>
    </sbe:message>

    <sbe:message name="OrderCancelledEvent" id="102"
                 description="Order successfully cancelled">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="cancelReason"    id="6"  type="CancelReason"/>
    </sbe:message>

    <sbe:message name="OrderAmendedEvent" id="103"
                 description="Order price or quantity successfully amended">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="newPrice"        id="6"  type="price"/>
        <field name="newQuantity"     id="7"  type="quantity"/>
    </sbe:message>

    <sbe:message name="OrderFilledEvent" id="104"
                 description="Order fully filled">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="fillPrice"       id="6"  type="price"/>
        <field name="fillQuantity"    id="7"  type="quantity"/>
    </sbe:message>

    <sbe:message name="OrderPartiallyFilledEvent" id="105"
                 description="Order partially filled, remainder open">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="accountId"       id="5"  type="accountId"/>
        <field name="fillPrice"       id="6"  type="price"/>
        <field name="fillQuantity"    id="7"  type="quantity"/>
        <field name="remainingQty"    id="8"  type="quantity"/>
    </sbe:message>

    <sbe:message name="CancelRejectedEvent" id="106"
                 description="Cancel request rejected (e.g. order already filled)">
        <field name="sequenceNumber"  id="1"  type="sequenceNumber"/>
        <field name="timestamp"       id="2"  type="timestamp"/>
        <field name="correlationId"   id="3"  type="correlationId"/>
        <field name="orderId"         id="4"  type="orderId"/>
        <field name="rejectReason"    id="5"  type="RejectReason"/>
    </sbe:message>

</sbe:messageSchema>
```

---

## Gradle Project Structure

```
oms-poc/
│
├── settings.gradle.kts
├── build.gradle.kts                    # root: shared plugins, allprojects repos
├── gradle/
│   └── libs.versions.toml
│
├── oms-sbe/                            # SBE schema + generated codecs
│   ├── src/main/resources/
│   │   └── oms-messages.xml
│   └── build.gradle.kts               # SBE codegen plugin runs here
│
├── oms-common/                         # Shared domain types, channel/stream constants, Agrona buffer utils
│   └── build.gradle.kts               # depends on: oms-sbe, agrona
│
├── oms-sequencer/                      # The two sequenced streams + Archive recording
│   └── build.gradle.kts               # depends on: oms-common, aeron, aeron-archive
│
├── oms-ingress/                        # External-facing: REST → SBE commands on IPC StreamId:10
│   └── build.gradle.kts               # depends on: oms-common, aeron, spring-boot (web only)
│
├── oms-order-aggregate/                # OrderAggregate: commands in, events out, state rebuilt from replay
│   └── build.gradle.kts               # depends on: oms-common, aeron, aeron-archive
│
├── oms-event-handlers/                 # Stateless event handlers (FillSimulator, Risk, etc.)
│   └── build.gradle.kts               # depends on: oms-common, aeron
│
├── oms-read-model/
│   ├── oms-read-model-database/        # Subscribes to events → H2/Postgres projection
│   │   └── build.gradle.kts           # depends on: oms-common, aeron, aeron-archive, jdbc
│   └── oms-read-model-viewserver/      # Subscribes to events → in-memory Map ViewServer
│       └── build.gradle.kts           # depends on: oms-common, aeron, aeron-archive
│
├── oms-api/                            # REST + WebSocket query layer over ViewServer / DB read model
│   └── build.gradle.kts               # depends on: oms-read-model-viewserver, spring-boot
│
└── oms-app/                            # Bootstraps everything: MediaDriver, Archive, all components
    └── build.gradle.kts               # depends on: all modules
```

### Dependency Flow

The dependency direction is strictly one-way — no circular dependencies are permitted:

```
oms-sbe
  └── oms-common
        ├── oms-sequencer
        ├── oms-ingress
        ├── oms-order-aggregate
        ├── oms-event-handlers
        ├── oms-read-model-database
        ├── oms-read-model-viewserver
        │     └── oms-api
        └── oms-app (depends on all of the above)
```

All inter-component communication goes via Aeron IPC using events and commands defined in `oms-sbe`. No sub-project depends on another sub-project's internals directly.

### `libs.versions.toml`

```toml
[versions]
aeron       = "1.44.1"
agrona      = "1.21.1"
sbe         = "1.30.0"
spring-boot = "3.3.0"
kotlin      = "1.9.24"

[libraries]
aeron-driver  = { module = "io.aeron:aeron-driver",  version.ref = "aeron" }
aeron-client  = { module = "io.aeron:aeron-client",  version.ref = "aeron" }
aeron-archive = { module = "io.aeron:aeron-archive", version.ref = "aeron" }
agrona        = { module = "org.agrona:agrona",       version.ref = "agrona" }
sbe-tool      = { module = "uk.co.real-logic:sbe-tool", version.ref = "sbe" }

[plugins]
sbe           = { id = "uk.co.real-logic.sbe", version.ref = "sbe" }
```

---

## Key Design Details

### Channels & Stream IDs

Defined centrally in `oms-common` — no magic numbers anywhere else in the codebase:

```kotlin
object OmsStreams {
    const val IPC = "aeron:ipc"

    // Pre-sequencer ingress — written by Ingress, Aggregate, and EventHandlers
    const val COMMAND_INGRESS_STREAM = 10
    const val EVENT_INGRESS_STREAM   = 11

    // Canonical sequenced streams — written ONLY by the Sequencer
    const val COMMAND_STREAM         = 1
    const val EVENT_STREAM           = 2
}
```

For the PoC, everything runs over `aeron:ipc` (same process). Switching to a distributed deployment is purely a configuration change — replace `aeron:ipc` with `aeron:udp?endpoint=<host>:<port>` with no code changes in any component.

### OrderAggregate Threading Model

Because the aggregate now applies state changes synchronously within command handling, it requires only a **single `AgentRunner` thread** using Agrona's `Agent` interface — no thread blocking, pure busy-spin or back-off idle strategy:

- **CommandAgent** — polls the Command Stream, runs `handleCommand()`, publishes the resulting event to Event Ingress, then immediately calls `applyEvent()` to update state — all within the same poll cycle before returning to poll again.

There is no separate `EventAgent` for live operation. This single-threaded model eliminates all concurrency concerns on the hot path entirely. State is only ever mutated by one thread, in a strictly sequential order that mirrors the sequence of commands received.

During startup replay, the same `applyEvent()` function is called from the replay loop on the same thread, meaning the state reconstruction logic is identical to the live state update logic — there is no separate code path to maintain or diverge.

```kotlin
// Single agent — handles both replay and live operation
class OrderAggregateAgent : Agent {

    override fun doWork(): Int {
        return commandSubscription.poll(commandHandler, MAX_POLL_FRAGMENTS)
    }

    private val commandHandler = FragmentHandler { buffer, offset, length, header ->
        val templateId = messageHeader.wrap(buffer, offset).templateId()
        val event = when (templateId) {
            NewOrderCommand.TEMPLATE_ID    -> handleNewOrder(buffer, offset)
            CancelOrderCommand.TEMPLATE_ID -> handleCancel(buffer, offset)
            AmendOrderCommand.TEMPLATE_ID  -> handleAmend(buffer, offset)
            else -> return@FragmentHandler
        }
        eventPublication.offer(event)   // publish to Event Ingress
        applyEvent(event)               // update state immediately
    }
}
```

### Startup Replay Sequence

The aggregate uses the Aeron Archive exclusively during startup to rebuild state, then closes the replay subscription before beginning live command processing. The sequence is:

```
1. Connect to AeronArchive
2. Record the current stopPosition of the Event Stream recording
3. Create a replay subscription from position 0 to stopPosition
4. Poll the replay subscription, applying all events via applyEvent()
5. Once replay reaches stopPosition, close the replay subscription
6. Begin polling the live Command Stream — CommandAgent starts accepting live commands
```

Because the aggregate no longer subscribes to the live Event Stream at all, there is no gap-closure concern between replay and live mode. The Archive replay brings state fully up to date, and from that point command processing with inline `applyEvent()` keeps it current. The Command Stream naturally buffers any commands that arrived during the replay window in the Aeron subscription, so they are processed in order immediately after replay completes with no special handling required.

### MediaDriver Strategy

For the PoC, a single **embedded `MediaDriver`** launched from `oms-app` serves all components in the same process. When splitting into separate processes, switch to a **standalone MediaDriver** running as a dedicated process — all components then connect to it with no code changes in the components themselves.

```kotlin
// PoC: embedded driver
val mediaDriver = MediaDriver.launchEmbedded()
val aeron = Aeron.connect(Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()))

// Distributed: standalone driver, components connect independently
val aeron = Aeron.connect(Aeron.Context().aeronDirectoryName("/dev/shm/aeron"))
```

---

## PoC Suggested Milestones

**Milestone 1 — Core Skeleton**
Set up Gradle multi-project, define `oms-sbe` schema with codegen, define `OmsStreams` constants in `oms-common`, wire the `Sequencer`, stub all other components with logging-only implementations.

**Milestone 2 — Happy Path**
Full end-to-end flow: REST ingress → `NewOrderCommand` → Sequencer → `OrderAggregate` validates → `OrderAcceptedEvent` → `FillSimulatorHandler` → `OrderFilledEvent` → `ViewServerReadModel` updated.

**Milestone 3 — Order State Machine**
All state transitions implemented: partial fills, cancellation, amendment, rejection. All invalid transitions produce appropriate rejected events.

**Milestone 4 — Archive & Replay**
`OrderAggregate` rebuilds state on startup from Archive replay. `DatabaseReadModel` and `ViewServerReadModel` replay from checkpoint on restart.

**Milestone 5 — API & Query Layer**
REST query endpoints and WebSocket push from `ViewServerReadModel`. Full Swagger documentation on the Ingress API.