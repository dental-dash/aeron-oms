# FIX Integration Architecture Design

## Aeron OMS — Artio FIX Engine Integration

**Document scope:** How to integrate Aeron Artio as the order ingress transport, replacing a direct REST API, while keeping FIX protocol details hidden from the core `OrderAggregate` domain.

---

## 1. Architectural Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  FIX Client Process                                                     │
│                                                                         │
│  REST Client ──► OrderController ──► FixInitiatorService               │
│                                            │                            │
│                                    Artio Initiator                      │
│                                    (FIX 4.4 / TCP)                      │
└────────────────────────────────────────────│────────────────────────────┘
                                             │  TCP (FIX wire protocol)
                                             │
┌────────────────────────────────────────────│────────────────────────────┐
│  OMS Process                               │                            │
│                                            ▼                            │
│                                    Artio Acceptor                       │
│                                    (FixSessionHandler)                  │
│                                            │                            │
│                            NewOrderSingleCommand / CancelOrderCommand   │
│                            (published to Aeron IPC Command Stream)      │
│                                            │                            │
│                                            ▼                            │
│                                       Sequencer                         │
│                                    (assigns sequence numbers)           │
│                                            │                            │
│                                            ▼                            │
│                              ┌─────────────────────────┐               │
│                              │  FixOrderAggregateAgent  │               │
│                              │                          │               │
│                              │  FIX state machine:      │               │
│                              │  - clOrdId → orderId map │               │
│                              │  - FIX session tracking  │               │
│                              │  - ExecutionReport out   │               │
│                              └────────────┬─────────────┘               │
│                                           │ PlaceOrderCommand           │
│                                           │ (internal, no FIX detail)   │
│                                           ▼                             │
│                                    OrderAggregate                       │
│                                    (pure domain logic)                  │
│                                           │                             │
│                                           ▼                             │
│                                     Event Stream                        │
│                                  (OrderPlacedEvent, etc.)               │
│                                           │                             │
│                              ┌────────────┘                             │
│                              │                                          │
│                              ▼                                          │
│                  FixOrderAggregateAgent                                  │
│                  (also subscribes to Event Stream                       │
│                   to send ExecutionReport back to client)               │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key design principles:**

1. The `OrderAggregate` never sees a FIX tag, a `clOrdId`, or a session identifier. FIX is a transport concern.
2. The `FixOrderAggregateAgent` owns the FIX state machine — it is the anti-corruption layer.
3. Everything flows through the `Sequencer` for deterministic ordering. The acceptor publishes to the Command Stream; it does not call the aggregate directly.
4. The `FixOrderAggregateAgent` has a dual role: it subscribes to the Command Stream (to manage FIX order state and forward internal commands) and to the Event Stream (to send `ExecutionReport` messages back to the FIX client).

---

## 2. Module Structure

```
oms-fix-integration/
├── fix-common/                   # Shared constants, FIX field mappings, IPC schemas
├── fix-client/                   # Spring Boot REST API + Artio Initiator
├── fix-acceptor/                 # Artio Acceptor — bridges FIX → Aeron Command Stream
└── fix-aggregate-agent/          # FixOrderAggregateAgent — FIX state machine + ACL
```

**Dependency graph** (no cycles):

```
fix-common  ◄──  fix-client
fix-common  ◄──  fix-acceptor
fix-common  ◄──  fix-aggregate-agent
```

`fix-aggregate-agent` and `fix-acceptor` run **in the same OMS JVM process** but are separate compilation units. `fix-client` runs in a separate process.

---

## 3. Command and Event Naming

Commands and events follow FIX message names directly. This makes the mapping from FIX wire messages to internal commands obvious and auditable.

### 3.1 Commands (FIX → Command Stream)

| FIX Message (wire) | Internal Command | Published by |
|--------------------|------------------|--------------|
| `NewOrderSingle` (35=D) | `NewOrderSingleCommand` | `FixSessionHandler` (acceptor) |
| `OrderCancelRequest` (35=F) | `OrderCancelRequestCommand` | `FixSessionHandler` (acceptor) |
| `OrderCancelReplaceRequest` (35=G) | `OrderCancelReplaceRequestCommand` | `FixSessionHandler` (acceptor) |

These commands carry FIX-level fields (`clOrdId`, `origClOrdId`, `sessionId`). They are understood by `FixOrderAggregateAgent` only.

### 3.2 Internal Commands (FixOrderAggregateAgent → OrderAggregate)

After translation, the agent emits pure domain commands onto a separate **Internal Command Stream** (or forwards directly on the same stream with a different template ID):

| FIX Command | Internal Domain Command | Notes |
|-------------|------------------------|-------|
| `NewOrderSingleCommand` | `PlaceOrderCommand` | `clOrdId` mapped to internal `orderId` |
| `OrderCancelRequestCommand` | `CancelOrderCommand` | `origClOrdId` resolved to internal `orderId` |
| `OrderCancelReplaceRequestCommand` | `AmendOrderCommand` | New qty/price extracted |

### 3.3 Events (Event Stream → ExecutionReport back to FIX client)

| Internal Event | FIX Message Sent | ExecType |
|---------------|------------------|----------|
| `OrderPlacedEvent` | `ExecutionReport` | `0` (New) |
| `OrderFilledEvent` | `ExecutionReport` | `F` (Trade) |
| `OrderPartiallyFilledEvent` | `ExecutionReport` | `F` (Trade) |
| `OrderCancelledEvent` | `ExecutionReport` | `4` (Canceled) |
| `OrderRejectedEvent` | `ExecutionReport` | `8` (Rejected) |
| `OrderAmendedEvent` | `ExecutionReport` | `5` (Replace) |

---

## 4. FIX Acceptor — Bridging to the Aeron Command Stream

This is the most critical integration point. The acceptor receives FIX messages from Artio's session-management thread and must publish them onto the Aeron IPC Command Stream with zero blocking.

### 4.1 Threading model

Artio's `SessionHandler.onMessage()` is called on **Artio's framing thread**. The Aeron `Publication.offer()` is non-blocking (returns immediately with a back-pressure code). This is the correct combination — never call a blocking operation from `onMessage()`.

```
Artio framing thread
        │
        │  onMessage() callback
        ▼
FixSessionHandler.onNewOrderSingle()
        │
        │  Publication.offer(buffer, 0, length)  ← non-blocking IPC
        ▼
Aeron Command Stream (IPC)
        │
        ▼
Sequencer (stamps sequence number, re-publishes to sequenced Command Stream)
        │
        ▼
FixOrderAggregateAgent.onFragment()
```

### 4.2 Back-pressure handling

`Publication.offer()` returns a negative value when back-pressured (`BACK_PRESSURED`, `NOT_CONNECTED`, `ADMIN_ACTION`). The acceptor must handle this without losing messages.

**Recommended pattern — local offer-retry loop with bounded spin:**

```java
// In FixSessionHandler
private static final int MAX_OFFER_ATTEMPTS = 100;

private void offerCommand(final DirectBuffer buffer, final int length) {
    int attempts = 0;
    long result;
    do {
        result = commandPublication.offer(buffer, 0, length);
        if (result > 0) return;                         // success
        if (result == Publication.CLOSED) {
            throw new IllegalStateException("Command publication closed");
        }
        // BACK_PRESSURED or ADMIN_ACTION — spin briefly
        Thread.onSpinWait();
    } while (++attempts < MAX_OFFER_ATTEMPTS);

    // TODO(POC): production systems should enqueue to a bounded MPSC queue
    // and apply circuit-breaker logic (reject with FIX BusinessMessageReject)
    sendBusinessReject(sessionId, refMsgType, "System back-pressured");
}
```

For production: use an `ManyToOneConcurrentArrayQueue` (Agrona) as a staging buffer between the Artio thread and a dedicated Aeron publisher thread. This fully decouples the two thread domains.

### 4.3 SBE encoding in the acceptor

The acceptor encodes `NewOrderSingleCommand` directly into an `ExpandableArrayBuffer` using SBE-generated encoders. No heap allocation in the fast path.

```java
// Reuse these per-thread (ThreadLocal or single-threaded acceptor)
private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(512);
private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
private final NewOrderSingleCommandEncoder commandEncoder = new NewOrderSingleCommandEncoder();

public void onNewOrderSingle(final Session session,
                              final NewOrderSingleDecoder fixMsg) {

    commandEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
        .clOrdId(fixMsg.clOrdId())
        .sessionId(session.id())
        .symbol(fixMsg.symbol())
        .side(mapSide(fixMsg.side()))
        .ordType(mapOrdType(fixMsg.ordType()))
        .orderQty(fixMsg.orderQty().mantissa(), fixMsg.orderQty().exponent())
        .price(fixMsg.price().mantissa(), fixMsg.price().exponent())
        .transactTime(fixMsg.transactTime());

    final int length = MessageHeaderEncoder.ENCODED_LENGTH + commandEncoder.encodedLength();
    offerCommand(buffer, length);
}
```

### 4.4 Artio acceptor configuration

```java
EngineConfiguration configuration = new EngineConfiguration()
    .bindTo("0.0.0.0", 9880)                  // acceptor TCP port
    .libraryAeronChannel(IPC_CHANNEL)          // Artio internal Aeron channel
    .logAllMessages(false)                     // TODO(POC): enable in staging
    .authenticationStrategy(
        AuthenticationStrategy.of(ACCEPTED_COMP_IDS));  // whitelist initiators

// Share the MediaDriver with the OMS — do not create a second driver
FixEngine engine = FixEngine.launch(configuration);

LibraryConfiguration libraryConfig = new LibraryConfiguration()
    .sessionAcquireHandler(fixSessionAcquireHandler)
    .sessionExistsHandler(fixSessionExistsHandler)
    .libraryAeronChannel(IPC_CHANNEL);

FixLibrary library = FixLibrary.connect(libraryConfig);
```

**Important:** Artio manages its own internal Aeron streams. The Command Stream used to talk to the Sequencer is a **separate, application-level** Aeron publication — not Artio's internal channel.

---

## 5. FixOrderAggregateAgent

The agent runs as an `Agent` in the OMS `AgentRunner`, on its own pinned thread. It has two subscriptions:

1. **Sequenced Command Stream** — receives `NewOrderSingleCommand`, `OrderCancelRequestCommand`, etc.
2. **Event Stream** — receives `OrderPlacedEvent`, `OrderFilledEvent`, etc. to generate `ExecutionReport` replies.

### 5.1 FIX state machine responsibilities

```java
public class FixOrderAggregateAgent implements Agent, FragmentHandler {

    // FIX-level state — hidden from OrderAggregate
    private final Long2ObjectHashMap<FixOrderState> fixStateBySessionId;
    private final Object2LongHashMap<String> orderIdByClOrdId;   // clOrdId → internal orderId
    private final Long2ObjectHashMap<String> clOrdIdByOrderId;   // internal orderId → clOrdId (for ExecReports)

    // Artio session reference for sending ExecutionReports back
    private final FixLibrary fixLibrary;
    private Session fixSession; // acquired via SessionAcquireHandler

    // Publication to the Sequencer's Command Stream
    // (for forwarding PlaceOrderCommand after FIX state is recorded)
    private final Publication internalCommandPublication;
}
```

### 5.2 Command Stream handler — receiving FIX commands

```java
@Override
public void onFragment(final DirectBuffer buffer,
                        final int offset,
                        final int length,
                        final Header header) {

    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();

    switch (templateId) {
        case NewOrderSingleCommandDecoder.TEMPLATE_ID  -> onNewOrderSingleCommand(buffer, offset);
        case OrderCancelRequestCommandDecoder.TEMPLATE_ID -> onOrderCancelRequestCommand(buffer, offset);
        // internal commands are routed to OrderAggregate by a separate handler
    }
}

private void onNewOrderSingleCommand(final DirectBuffer buffer, final int offset) {
    commandDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

    final String clOrdId    = commandDecoder.clOrdId();
    final long   sessionId  = commandDecoder.sessionId();

    // 1. Validate: reject duplicate clOrdId
    if (orderIdByClOrdId.containsKey(clOrdId)) {
        sendExecutionReport(sessionId, clOrdId, ExecType.REJECTED, "Duplicate clOrdId");
        return;
    }

    // 2. Generate internal orderId (sequencer-assigned or UUID — POC uses UUID)
    final long internalOrderId = nextOrderId();

    // 3. Record FIX state BEFORE forwarding (synchronous, same thread)
    orderIdByClOrdId.put(clOrdId, internalOrderId);
    clOrdIdByOrderId.put(internalOrderId, clOrdId);
    fixStateBySessionId.put(internalOrderId,
        new FixOrderState(clOrdId, sessionId, FixOrdStatus.PENDING_NEW));

    // 4. Forward as internal PlaceOrderCommand (no FIX fields)
    publishPlaceOrderCommand(internalOrderId, commandDecoder);
}
```

**Critical:** Steps 3 and 4 happen synchronously within `onFragment()`. The FIX state is recorded before the internal command is published — this mirrors the pattern used in `OrderAggregate` for its own state updates.

### 5.3 Event Stream handler — sending ExecutionReports

```java
private void onOrderPlacedEvent(final DirectBuffer buffer, final int offset) {
    eventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

    final long   internalOrderId = eventDecoder.orderId();
    final String clOrdId         = clOrdIdByOrderId.get(internalOrderId);

    if (clOrdId == null) {
        // Order did not originate from FIX — ignore
        return;
    }

    final FixOrderState state = fixStateBySessionId.get(internalOrderId);
    state.updateStatus(FixOrdStatus.NEW);

    sendExecutionReport(
        state.sessionId(),
        clOrdId,
        internalOrderId,
        ExecType.NEW,
        OrdStatus.NEW,
        eventDecoder.orderQty(),
        Decimal.ZERO,     // cumQty
        eventDecoder.orderQty()  // leavesQty
    );
}
```

### 5.4 ExecutionReport construction (Artio)

The `ExecutionReport` is sent back via the Artio `FixLibrary` using SBE-generated FIX encoders. This happens on the `FixOrderAggregateAgent` thread — not the Artio framing thread — which is safe because `FixLibrary.sessions()` returns sessions that can be written to from any thread via `Session.trySend()`.

```java
private void sendExecutionReport(final long sessionId,
                                  final String clOrdId,
                                  final long orderId,
                                  final char execType,
                                  final char ordStatus,
                                  ...) {

    final Session session = sessionFor(sessionId);
    if (session == null || !session.isActive()) {
        // TODO(POC): queue for replay when session reconnects
        return;
    }

    execReportEncoder.reset()
        .execId(nextExecId())
        .clOrdId(clOrdId)
        .orderId(String.valueOf(orderId))
        .execType(execType)
        .ordStatus(ordStatus)
        .symbol(symbol)
        .side(side)
        .leavesQty(leavesQty)
        .cumQty(cumQty)
        .avgPx(avgPx);

    final long result = session.trySend(execReportEncoder);
    if (result < 0) {
        // TODO(POC): handle send failure — log and queue
    }
}
```

---

## 6. Aeron Stream Layout

```
Channel: aeron:ipc

Stream 1 — Raw Command Stream
  Publisher:  FixSessionHandler (acceptor, Artio thread)
  Subscriber: Sequencer

Stream 2 — Sequenced Command Stream
  Publisher:  Sequencer
  Subscriber: FixOrderAggregateAgent   (FIX commands)
              OrderAggregate           (internal domain commands)

Stream 3 — Event Stream
  Publisher:  OrderAggregate
  Subscriber: FixOrderAggregateAgent   (to send ExecReports)
              ReadModels               (projections)
              Aeron Archive            (persistence)
```

**Why two command streams?** The Raw Command Stream carries FIX-level commands (`NewOrderSingleCommand`). After sequencing, the Sequenced Command Stream carries both FIX commands (consumed only by `FixOrderAggregateAgent`) and internal commands (`PlaceOrderCommand`) consumed by `OrderAggregate`. Template IDs distinguish them. Alternatively, use a single Command Stream with the sequencer stamping all messages — simpler and preferred for the POC.

**Single-stream alternative (recommended for POC):**

```
Stream 1 — Command Stream (single)
  Publisher:  FixSessionHandler  (FIX commands, templateId=NewOrderSingleCommand)
              FixOrderAggregateAgent (internal commands, templateId=PlaceOrderCommand)
  Subscriber: Sequencer  →  stamps sequence number  →  re-publishes to Stream 2

Stream 2 — Sequenced Command Stream
  Subscriber: FixOrderAggregateAgent  (handles FIX templateIds only)
              OrderAggregate          (handles internal templateIds only)
```

---

## 7. SBE Schema — FIX Command Templates

```xml
<!-- NewOrderSingleCommand — mirrors FIX 35=D fields needed by agent -->
<sbe:message name="NewOrderSingleCommand" id="10" description="FIX NewOrderSingle from acceptor">
    <field name="sessionId"    id="1" type="int64"  description="Artio session identifier"/>
    <field name="clOrdId"      id="2" type="string" length="36" description="FIX tag 11"/>
    <field name="symbol"       id="3" type="string" length="20" description="FIX tag 55"/>
    <field name="side"         id="4" type="SideEnum"           description="FIX tag 54"/>
    <field name="ordType"      id="5" type="OrdTypeEnum"        description="FIX tag 40"/>
    <field name="orderQty"     id="6" type="Decimal64"          description="FIX tag 38"/>
    <field name="price"        id="7" type="Decimal64"          description="FIX tag 44"/>
    <field name="account"      id="8" type="string" length="20" description="FIX tag 1"/>
    <field name="transactTime" id="9" type="int64"              description="FIX tag 60, epoch nanos"/>
</sbe:message>

<!-- OrderCancelRequestCommand — mirrors FIX 35=F -->
<sbe:message name="OrderCancelRequestCommand" id="11" description="FIX OrderCancelRequest">
    <field name="sessionId"     id="1" type="int64"/>
    <field name="clOrdId"       id="2" type="string" length="36" description="FIX tag 11"/>
    <field name="origClOrdId"   id="3" type="string" length="36" description="FIX tag 41"/>
    <field name="symbol"        id="4" type="string" length="20"/>
    <field name="side"          id="5" type="SideEnum"/>
    <field name="transactTime"  id="6" type="int64"/>
</sbe:message>

<!-- PlaceOrderCommand — internal, no FIX tags -->
<sbe:message name="PlaceOrderCommand" id="20" description="Internal domain command">
    <field name="orderId"   id="1" type="int64"/>
    <field name="symbol"    id="2" type="string" length="20"/>
    <field name="side"      id="3" type="SideEnum"/>
    <field name="ordType"   id="4" type="OrdTypeEnum"/>
    <field name="orderQty"  id="5" type="Decimal64"/>
    <field name="price"     id="6" type="Decimal64"/>
</sbe:message>

<!-- CancelOrderCommand — internal -->
<sbe:message name="CancelOrderCommand" id="21" description="Internal cancel command">
    <field name="orderId"   id="1" type="int64"/>
    <field name="symbol"    id="2" type="string" length="20"/>
    <field name="side"      id="3" type="SideEnum"/>
</sbe:message>
```

Template ID ranges:
- `10–19` — FIX-level commands (handled only by `FixOrderAggregateAgent`)
- `20–29` — Internal domain commands (handled only by `OrderAggregate`)
- `30–39` — Internal domain events (handled by all event subscribers)

---

## 8. FixOrderState — Internal FIX State Machine

`FixOrderAggregateAgent` maintains a per-order `FixOrderState` object. This is the only place where FIX-specific lifecycle is tracked.

```
FixOrderState transitions:

PENDING_NEW
    │  PlaceOrderCommand forwarded, awaiting OrderPlacedEvent
    ▼
NEW
    │  ExecutionReport(ExecType=0, OrdStatus=0) sent
    ├──► PARTIALLY_FILLED
    │       │  ExecutionReport(ExecType=F, OrdStatus=1) sent
    │       ▼
    │    FILLED
    │       │  ExecutionReport(ExecType=F, OrdStatus=2) sent
    ▼
PENDING_CANCEL
    │  CancelOrderCommand forwarded
    ▼
CANCELED
    │  ExecutionReport(ExecType=4, OrdStatus=4) sent
    ▼
REJECTED
       ExecutionReport(ExecType=8, OrdStatus=8) sent
```

State objects are held in `Long2ObjectHashMap` (Agrona, no autoboxing) keyed by internal `orderId`.

---

## 9. FIX Client — REST to FIX Initiator

The client is a Spring Boot application with a thin REST layer. The `FixInitiatorService` wraps an Artio `FixLibrary` configured as an initiator.

```
POST /api/v1/orders
Body: { "symbol": "AAPL", "side": "1", "ordType": "2", "orderQty": 100, "price": 185.50 }

OrderController
    │  generates clOrdId (UUID)
    │  validates request
    ▼
FixInitiatorService.sendNewOrderSingle(clOrdId, request)
    │  encodes FIX NewOrderSingle via Artio encoder
    ▼
Artio Initiator Session.trySend(newOrderSingleEncoder)
    │  TCP
    ▼
OMS Artio Acceptor
```

**Response:** The REST call returns `202 Accepted` with the `clOrdId`. Execution Reports arrive asynchronously. For the POC, the client can subscribe to a SSE endpoint (`GET /api/v1/orders/{clOrdId}/events`) backed by a `ConcurrentHashMap<String, SseEmitter>` populated when the initiator's session handler receives an `ExecutionReport`.

```
POST /api/v1/orders  →  202 Accepted  { "clOrdId": "abc-123" }
GET  /api/v1/orders/abc-123/events  →  SSE stream
    data: {"execType":"0","ordStatus":"0","leavesQty":100}   ← New
    data: {"execType":"F","ordStatus":"2","cumQty":100}       ← Filled
```

---

## 10. Key Design Decisions and Rationale

### Why publish FIX commands to the Sequencer rather than calling the agent directly?

All state changes in the system must be ordered deterministically. If the acceptor called the `FixOrderAggregateAgent` directly (bypassing the sequencer), concurrent commands from multiple FIX sessions would race. Publishing to the Command Stream and processing via the sequencer gives a total order over all commands, which is essential for correctness and for replay via Aeron Archive.

### Why does FixOrderAggregateAgent sit between the Sequencer and OrderAggregate?

The agent consumes `NewOrderSingleCommand` (a FIX-level message), performs the `clOrdId` → `orderId` translation, records the FIX session association, and only then publishes `PlaceOrderCommand` (a domain-level message). The `OrderAggregate` only ever sees `PlaceOrderCommand`. This is the anti-corruption layer pattern: FIX protocol semantics do not leak into the core domain.

### Why not use a shared SBE schema with optional FIX fields on PlaceOrderCommand?

Adding `clOrdId` and `sessionId` to `PlaceOrderCommand` would pollute the internal domain model. When a second order channel is added (e.g. proprietary binary API, GUI), those commands would also need to carry FIX fields — or the schema would need version conditionals. Keeping FIX fields in a separate template hierarchy is cleaner and more extensible.

### CharSequence vs String in SBE callbacks

Use `CharSequence` (Artio returns `AsciiSequenceView`) throughout the acceptor and agent hot paths. Converting to `String` allocates on the heap and will eventually trigger GC pauses. Only convert to `String` at the boundary where data enters a data structure that requires it (e.g. `orderIdByClOrdId` map). In the POC this is acceptable; in production use Agrona's `MutableAsciiBuffer` pooling.

### Artio library thread vs agent thread for sending ExecutionReports

Artio's `Session.trySend()` is thread-safe. The `FixOrderAggregateAgent` runs on its own pinned thread (Aeron `AgentRunner`) and can safely call `trySend()` without synchronisation. Do not call `trySend()` from within `onMessage()` callbacks (Artio framing thread) as this creates a re-entrant send path.

---

## 11. Failure Scenarios

| Scenario | Behaviour |
|----------|-----------|
| FIX session disconnects before ExecutionReport sent | Store pending ExecReports keyed by `sessionId`; replay on reconnect via `SessionAcquireHandler.onSessionAcquired()` |
| Command Stream back-pressure | Acceptor spins up to N attempts; on exhaustion sends `BusinessMessageReject` (35=j) back to initiator |
| Duplicate `clOrdId` | Agent detects duplicate in `orderIdByClOrdId`; sends `ExecutionReport` with `ExecType=8` (Rejected), `Text="Duplicate clOrdId"` |
| OrderAggregate rejects PlaceOrderCommand | `OrderRejectedEvent` on Event Stream; agent maps back to `clOrdId` and sends `ExecutionReport(ExecType=8)` |
| OMS process restart | Replay Command Stream from Aeron Archive; agent rebuilds `orderIdByClOrdId` and `fixStateBySessionId` maps before allowing new FIX connections |

---

## 12. File and Class Layout

```
fix-common/
  FixTags.java                    Constants for all FIX tag numbers and values
  FixNewOrderRequest.java         Canonical FIX order model (used in fix-client only)

fix-acceptor/
  FixAcceptorMain.java            Process entry point; wires Artio + Aeron
  FixSessionHandler.java          Artio SessionHandler; encodes + publishes to Command Stream
  FixSessionAcquireHandler.java   Tracks active sessions for ExecutionReport routing
  CommandStreamPublisher.java     Wraps Publication.offer() with back-pressure retry

fix-aggregate-agent/
  FixOrderAggregateAgent.java     Agent implementation; Command + Event stream subscriber
  FixOrderState.java              Per-order FIX state (clOrdId, sessionId, FixOrdStatus)
  FixCommandTranslator.java       NewOrderSingleCommand → PlaceOrderCommand translation
  ExecutionReportSender.java      Builds and sends FIX ExecutionReport via FixLibrary
  FixOrdStatus.java               Enum matching FIX OrdStatus (39) values

fix-client/
  FixClientMain.java              Spring Boot entry point
  OrderController.java            REST controller; generates clOrdId; calls FixInitiatorService
  FixInitiatorService.java        Artio initiator lifecycle; Session.trySend()
  OrderEventsController.java      SSE endpoint for async ExecutionReport delivery
  FixClientSessionHandler.java    Artio callback; routes ExecReports to SSE emitters
  PlaceOrderRequest.java          REST request DTO (validated)
```

---

## 13. PoC Suggested Milestones

Each milestone produces a runnable, verifiable slice of the system. Later milestones build directly on earlier ones — no throwaway scaffolding.

---

### Milestone 1 — Gradle Multi-Project Shell and SBE Schema

**Goal:** The build compiles cleanly; all SBE-generated codecs are on the classpath.

**Tasks:**
- Create the four Gradle subprojects (`fix-common`, `fix-client`, `fix-acceptor`, `fix-aggregate-agent`) with dependency graph as per Section 2.
- Author the SBE schema in `fix-common` covering:
  - `NewOrderSingleCommand` (template ID 10)
  - `OrderCancelRequestCommand` (template ID 11)
  - `PlaceOrderCommand` (template ID 20)
  - `CancelOrderCommand` (template ID 21)
  - `OrderPlacedEvent` (template ID 30), `OrderRejectedEvent` (template ID 31)
- Wire the SBE Gradle plugin to generate encoders/decoders into `fix-common/build/generated`.
- Add `FixTags.java` constants.

**Verification:** `./gradlew build` passes; generated codec classes are visible in all subprojects.

---

### Milestone 2 — Artio FIX Session Handshake (Logon/Logout)

**Goal:** An Artio initiator and acceptor complete a FIX Logon (35=A) / Logout (35=5) exchange over localhost TCP with no application messages yet.

**Tasks:**
- Implement `FixAcceptorMain` — launch an embedded `MediaDriver`, start `FixEngine` bound to port 9880, connect a `FixLibrary` with a stub `FixSessionHandler` that logs all callbacks.
- Implement `FixClientMain` (plain Java, no Spring yet) — launch `FixEngine` + `FixLibrary` as initiator, connect to `localhost:9880`, log `onLogon` / `onLogout`.
- Configure Artio `SessionSettings` on both sides: `SenderCompID`, `TargetCompID`, `HeartBtInt`.
- Verify Artio's internal Aeron IPC channel (`aeron:ipc`) does not conflict with the application Command Stream (use different stream IDs).

**Verification:** Both sides log a successful Logon; a clean Logout on CTRL-C. No application messages sent.

---

### Milestone 3 — Acceptor Publishes NewOrderSingleCommand to Aeron

**Goal:** A hardcoded `NewOrderSingle` from the initiator travels over FIX and arrives on the Aeron Command Stream as a correctly encoded `NewOrderSingleCommand` SBE message.

**Tasks:**
- Implement `FixSessionHandler.onNewOrderSingle()` — decode the Artio FIX message, encode `NewOrderSingleCommand` via SBE into an `ExpandableArrayBuffer`, call `CommandStreamPublisher.offer()`.
- Implement `CommandStreamPublisher` with the bounded spin-retry back-pressure pattern (Section 4.2).
- Add a throwaway `CommandStreamDumper` subscriber (single class, `main()` method) that subscribes to the Command Stream and prints each decoded `NewOrderSingleCommand` to stdout — used only for this milestone, deleted after Milestone 4.
- In the initiator, send one hardcoded `NewOrderSingle` on `onLogon`.

**Verification:** `CommandStreamDumper` prints the `clOrdId`, `symbol`, `side`, `orderQty`, and `price` matching the hardcoded values. Artio session stays connected throughout.

---

### Milestone 4 — Sequencer Stamps and Re-Publishes Commands

**Goal:** The Sequencer sits between the Raw Command Stream and the Sequenced Command Stream, assigning monotonically increasing sequence numbers.

**Tasks:**
- Port or reuse the existing `Sequencer` from the main OMS project; wire it to consume Stream 1 and publish to Stream 2.
- Update `CommandStreamDumper` to subscribe to Stream 2 (the sequenced stream) and verify the sequence number field is populated.
- Confirm that the `sessionId` and all FIX-level fields survive the sequencer pass-through unchanged (sequencer only prepends the sequence header, does not modify the body).

**Verification:** `CommandStreamDumper` on Stream 2 shows sequence numbers starting at 1 and incrementing per message. Send three orders in quick succession; verify no gaps.

---

### Milestone 5 — FixOrderAggregateAgent — Command Handling and Translation

**Goal:** The `FixOrderAggregateAgent` consumes `NewOrderSingleCommand` from the Sequenced Command Stream, records FIX state, and publishes a `PlaceOrderCommand` onto the same stream (or a dedicated internal stream).

**Tasks:**
- Implement `FixOrderAggregateAgent` as an Agrona `Agent`; wire into an `AgentRunner` on a dedicated thread in the OMS process.
- Implement `FixOrderState`, `orderIdByClOrdId`, and `clOrdIdByOrderId` maps (Agrona `Object2LongHashMap` / `Long2ObjectHashMap`).
- Implement `FixCommandTranslator.translate(NewOrderSingleCommandDecoder) → PlaceOrderCommand` — generate an internal `orderId` (incrementing long for POC), record both maps, publish `PlaceOrderCommand`.
- Add duplicate `clOrdId` detection — log a warning and skip publication (ExecutionReport rejection comes in Milestone 7).

**Verification:** Add a second throwaway subscriber on the internal command stream; verify `PlaceOrderCommand` appears with the correct `orderId` and no FIX fields. Confirm `OrderAggregate` (if already wired) receives `PlaceOrderCommand` only.

---

### Milestone 6 — OrderAggregate Processes PlaceOrderCommand and Emits Events

**Goal:** The existing `OrderAggregate` processes `PlaceOrderCommand` and publishes `OrderPlacedEvent` (or `OrderRejectedEvent`) to the Event Stream without any FIX-specific changes.

**Tasks:**
- Confirm `OrderAggregate` handles `PlaceOrderCommand` template ID 20 — no changes should be needed if the existing aggregate already handles this command type.
- If not yet present, add minimal `OrderAggregate` logic: validate qty > 0 and symbol non-empty; publish `OrderPlacedEvent` on success, `OrderRejectedEvent` on failure.
- Wire the `OrderAggregate` `AgentRunner` into the OMS process alongside `FixOrderAggregateAgent`.

**Verification:** After sending a `NewOrderSingle` from the initiator, an `OrderPlacedEvent` appears on the Event Stream (verify with a throwaway subscriber). Send an invalid order (qty = 0); verify `OrderRejectedEvent` appears instead.

---

### Milestone 7 — FixOrderAggregateAgent Sends ExecutionReport Back to Initiator

**Goal:** The `FixOrderAggregateAgent` subscribes to the Event Stream and sends a FIX `ExecutionReport` back to the originating FIX session for every `OrderPlacedEvent` and `OrderRejectedEvent`.

**Tasks:**
- Add Event Stream subscription to `FixOrderAggregateAgent.doWork()` — poll both the Command Stream subscription and the Event Stream subscription in the same duty cycle.
- Implement `ExecutionReportSender` — look up `clOrdId` and `sessionId` from `clOrdIdByOrderId`, call `session.trySend(execReportEncoder)`.
- Handle the `OrderRejectedEvent` path: send `ExecutionReport(ExecType=8, OrdStatus=8)` with rejection reason in `Text` (tag 58).
- Handle duplicate `clOrdId` detected in Milestone 5: now send `ExecutionReport(ExecType=8)` rather than just logging.
- In `FixClientMain`, implement `FixClientSessionHandler.onExecutionReport()` and log the received fields.

**Verification:** Full round-trip — send `NewOrderSingle` from initiator, observe `ExecutionReport(ExecType=0)` logged on the initiator side. Send a bad order; observe `ExecutionReport(ExecType=8)`.

---

### Milestone 8 — OrderCancelRequest Flow

**Goal:** The cancel flow works end-to-end: `OrderCancelRequest` → `OrderCancelRequestCommand` → `CancelOrderCommand` → `OrderCancelledEvent` → `ExecutionReport(ExecType=4)`.

**Tasks:**
- Implement `FixSessionHandler.onOrderCancelRequest()` — encode `OrderCancelRequestCommand` and publish to Command Stream.
- Add `OrderCancelRequestCommand` handling in `FixOrderAggregateAgent`:
  - Resolve `origClOrdId` to internal `orderId` via `orderIdByClOrdId`; if not found, send `OrderCancelReject` (35=9).
  - Publish `CancelOrderCommand` with resolved `orderId`.
- Add `OrderCancelledEvent` handling in `FixOrderAggregateAgent` → `ExecutionReport(ExecType=4, OrdStatus=4)`.
- Add `CancelOrderCommand` handling in `OrderAggregate` if not already present.

**Verification:** Place an order, then cancel it. Initiator receives `ExecutionReport(New)` then `ExecutionReport(Canceled)`. Attempt to cancel an unknown `clOrdId`; verify `OrderCancelReject` is received.

---

### Milestone 9 — Spring Boot REST Client

**Goal:** Orders can be submitted via `POST /api/v1/orders` over HTTP; responses arrive via SSE.

**Tasks:**
- Add Spring Boot to `fix-client`; implement `OrderController` and `PlaceOrderRequest` DTO with Bean Validation.
- Implement `FixInitiatorService` as a Spring `@Service` — manage Artio `FixLibrary` lifecycle as a Spring-managed bean, send `NewOrderSingle` on request.
- Implement `OrderEventsController` with SSE: register a `SseEmitter` keyed by `clOrdId` on POST; complete it when `ExecutionReport` arrives in `FixClientSessionHandler`.
- Wire `FixClientSessionHandler` to look up the emitter and push the `ExecutionReport` payload as a JSON event.

**Verification:** `curl -X POST /api/v1/orders -d '{"symbol":"AAPL","side":"1","ordType":"2","orderQty":100,"price":185.50}'` returns `202 Accepted` with `clOrdId`. A concurrent `curl -N /api/v1/orders/{clOrdId}/events` receives the SSE `ExecutionReport` event within milliseconds.

---

### Milestone 10 — Aeron Archive Replay Verification

**Goal:** Confirm the system recovers correctly after an OMS restart by replaying the Command Stream from Aeron Archive and rebuilding `FixOrderAggregateAgent` state.

**Tasks:**
- Ensure the Command Stream is recorded by Aeron Archive (wire `ArchivingMediaDriver` if not already done in the base OMS).
- Implement `FixOrderAggregateAgent.replayAndCatchUp()` — on startup, request a bounded replay from position 0 to the current live position before joining the live subscription.
- Verify that `orderIdByClOrdId` and `clOrdIdByOrderId` are correctly rebuilt from replayed `NewOrderSingleCommand` and `OrderCancelRequestCommand` messages.
- After catchup, gate new FIX Logon acceptance (return `false` from `AuthenticationStrategy.authenticate()` until replay is complete).

**Verification:** Place three orders, stop the OMS, restart it. Replay completes; agent state matches pre-restart state. Place a fourth order; the system behaves correctly without a duplicate `orderId`.

---

### Milestone Summary

| # | Milestone | Primary Deliverable |
|---|-----------|-------------------|
| 1 | Gradle shell + SBE schema | Compiling multi-project build with generated codecs |
| 2 | Artio Logon/Logout | FIX session established over TCP |
| 3 | Acceptor → Aeron | `NewOrderSingleCommand` on Command Stream |
| 4 | Sequencer | Sequence-stamped commands on Stream 2 |
| 5 | FixOrderAggregateAgent (commands) | `PlaceOrderCommand` published, FIX state recorded |
| 6 | OrderAggregate events | `OrderPlacedEvent` on Event Stream |
| 7 | ExecutionReport round-trip | FIX `ExecutionReport` back to initiator |
| 8 | Cancel flow | Full cancel round-trip with `OrderCancelReject` |
| 9 | Spring Boot REST client | HTTP → FIX → SSE ExecutionReport |
| 10 | Archive replay | OMS restart recovers without state loss |
