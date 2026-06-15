# 03 — Simple Binary Encoding (SBE)

## What Is SBE?

Simple Binary Encoding (SBE) is a binary message serialisation library designed for
financial and low-latency systems. It was created by Real Logic and is now an FIX
protocol standard. SBE generates Java (and C++) codec classes from an XML schema, and
those codecs encode/decode directly on top of Agrona `DirectBuffer`s — with zero object
allocation and no intermediate copies.

GitHub: https://github.com/real-logic/simple-binary-encoding

---

## Why Not Use Protobuf, JSON, or Kryo?

| Serialiser | Allocates on encode/decode | Copy-free | Latency |
|------------|---------------------------|-----------|---------|
| JSON | Yes (strings, arrays) | No | High |
| Protobuf | Yes (objects) | No | Moderate |
| Kryo | Configurable | No | Moderate |
| **SBE** | **No** | **Yes** | **Lowest** |

SBE encodes values in-place into the buffer you provide, using CPU-native byte order
(little-endian by default for x86). The decoder reads directly from the network/IPC
buffer without copying it. This is what makes it the natural partner for Aeron.

---

## How It Works

### 1. Define a Schema (XML)

You describe your messages in an XML schema file. SBE uses this to generate codec
classes at build time.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe"
                   package="com.example.oms.sbe"
                   id="1"
                   version="1"
                   semanticVersion="1.0"
                   description="OMS Messages">

    <types>
        <type name="OrderId"   primitiveType="int64"/>
        <type name="Price"     primitiveType="double"/>
        <type name="Quantity"  primitiveType="int64"/>
        <enum name="Side" encodingType="uint8">
            <validValue name="BUY">0</validValue>
            <validValue name="SELL">1</validValue>
        </enum>
        <enum name="OrderType" encodingType="uint8">
            <validValue name="MARKET">0</validValue>
            <validValue name="LIMIT">1</validValue>
        </enum>
    </types>

    <sbe:message name="NewOrderSingle" id="1" description="New order request">
        <field name="orderId"   id="1" type="OrderId"/>
        <field name="price"     id="2" type="Price"/>
        <field name="quantity"  id="3" type="Quantity"/>
        <field name="side"      id="4" type="Side"/>
        <field name="orderType" id="5" type="OrderType"/>
    </sbe:message>

    <sbe:message name="ExecutionReport" id="2" description="Order execution confirmation">
        <field name="orderId"      id="1" type="OrderId"/>
        <field name="execQuantity" id="2" type="Quantity"/>
        <field name="execPrice"    id="3" type="Price"/>
    </sbe:message>

</sbe:messageSchema>
```

---

### 2. Generate Codec Classes

Add the SBE Gradle plugin (or Maven plugin) to generate source at build time:

```groovy
// build.gradle
plugins {
    id 'java'
}

configurations {
    sbeCodegen
}

dependencies {
    sbeCodegen "uk.co.real-logic:sbe-tool:1.30.0"
}

task generateSbe(type: JavaExec) {
    classpath = configurations.sbeCodegen
    mainClass = 'uk.co.real_logic.sbe.SbeTool'
    args = ['src/main/resources/oms-schema.xml']
    systemProperties = [
        'sbe.output.dir'      : 'src/generated/java',
        'sbe.target.language' : 'Java',
        'sbe.java.generate.interfaces' : 'true'
    ]
}

compileJava.dependsOn generateSbe
sourceSets.main.java.srcDir 'src/generated/java'
```

This generates classes like `NewOrderSingleEncoder`, `NewOrderSingleDecoder`,
`ExecutionReportEncoder`, `ExecutionReportDecoder`, plus a `MessageHeaderEncoder`/
`MessageHeaderDecoder` that carries the template ID and version.

---

### 3. Encoding a Message

```java
// Allocate ONCE and reuse
private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
private final NewOrderSingleEncoder orderEncoder = new NewOrderSingleEncoder();
private final UnsafeBuffer encodeBuffer =
    new UnsafeBuffer(ByteBuffer.allocateDirect(256));

public long sendNewOrder(Publication pub, long orderId, double price,
                         long qty, Side side, OrderType type) {

    // Wrap encoder onto our buffer starting at offset 0
    orderEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
        .orderId(orderId)
        .price(price)
        .quantity(qty)
        .side(side)
        .orderType(type);

    int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH
                      + orderEncoder.encodedLength();

    return pub.offer(encodeBuffer, 0, encodedLength);
}
```

Key points:
- `wrapAndApplyHeader()` writes the SBE message header (template ID, version, block
  length) and positions the encoder on the buffer. No objects are created.
- The fluent setter chain (`orderId(...).price(...).quantity(...)`) writes each field
  directly into the `encodeBuffer` at the correct byte offset.

---

### 4. Decoding a Message

```java
private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
private final NewOrderSingleDecoder orderDecoder = new NewOrderSingleDecoder();

// This is your Aeron fragment handler
private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {

    headerDecoder.wrap(buffer, offset);

    final int templateId = headerDecoder.templateId();
    final int actingVersion = headerDecoder.version();
    final int actingBlockLength = headerDecoder.blockLength();

    switch (templateId) {
        case NewOrderSingleDecoder.TEMPLATE_ID:
            orderDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                              actingBlockLength, actingVersion);

            long   orderId   = orderDecoder.orderId();
            double price     = orderDecoder.price();
            long   quantity  = orderDecoder.quantity();
            Side   side      = orderDecoder.side();

            processNewOrder(orderId, price, quantity, side);
            break;

        case ExecutionReportDecoder.TEMPLATE_ID:
            // ... decode execution report ...
            break;
    }
}
```

Key points:
- `wrap()` does not allocate. It stores the buffer reference and offset internally.
- You must check `templateId` to route to the right decoder.
- The decoder reads each field lazily when you call the getter — still zero allocation.

---

## Variable-Length Fields (Strings)

SBE handles variable-length strings with a length-prefixed encoding. Avoid these on
the ultra-hot path because reading them requires knowing the field's position from
the preceding fields. For the OMS POC, instrument symbols are a good candidate:

```xml
<sbe:message name="NewOrderSingle" id="1">
    ...
    <data name="symbol" id="6" type="varStringEncoding"/>
</sbe:message>
```

Encoding:
```java
orderEncoder.symbol("AAPL");
```

Decoding:
```java
String symbol = orderDecoder.symbol(); // allocates a String — acceptable for POC
// Zero-alloc alternative:
orderDecoder.getSymbol(byteArray, 0); // writes into a pre-allocated byte[]
```

---

## SBE in the OMS POC

The following messages should be defined in your schema:

| Message | Template ID | Direction |
|---------|-------------|-----------|
| `NewOrderSingle` | 1 | Gateway → Risk/Matching |
| `OrderCancelRequest` | 2 | Gateway → Risk/Matching |
| `OrderAmendRequest` | 3 | Gateway → Risk/Matching |
| `ExecutionReport` | 10 | Matching → Gateway |
| `OrderReject` | 11 | Risk → Gateway |

Keep all messages under 128 bytes where possible — this keeps them within a single
Ethernet frame and a single Aeron fragment.

---

## Common Mistakes

**Creating a new encoder per message.** Allocate `*Encoder` and `*Decoder` instances
once per thread and call `wrapAndApplyHeader()` / `wrap()` on each message to reposition
them on the buffer.

**Forgetting the message header.** Always use `wrapAndApplyHeader()` when encoding, and
read `headerDecoder.templateId()` before calling `wrap()` on a specific decoder. Without
the header, the receiver cannot determine which message type it received.

**Reading SBE fields outside the fragment handler.** The `DirectBuffer` passed to your
handler is only valid during the callback. If you need to process later, copy the
relevant fields into your own objects inside the handler.
