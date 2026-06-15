package com.oms.common;

/**
 * Central registry of all Aeron channel URIs and stream IDs used in the OMS.
 * No magic numbers anywhere else in the codebase — always reference these constants.
 *
 * <p>For the POC everything runs over aeron:ipc (same process). Switching to a
 * distributed deployment is a pure config change — replace IPC with a UDP URI.
 */
public final class OmsStreams {

    /** Shared-memory IPC channel for all POC communication. */
    public static final String IPC = "aeron:ipc";

    // ── Pre-sequencer ingress channels ──────────────────────────────────────
    // Written by external components; read exclusively by the Sequencer.

    /** New/amend/cancel commands from Order Ingress → Sequencer. */
    public static final int INGRESS_COMMAND_STREAM = 10;

    /** Domain events from OrderAggregate & EventHandlers → Sequencer. */
    public static final int INGRESS_EVENT_STREAM = 11;

    // ── Canonical sequenced streams ─────────────────────────────────────────
    // Written ONLY by the Sequencer; all other components are read-only subscribers.

    /** Sequenced command stream (StreamId 1) — read by OrderAggregate. */
    public static final int SEQUENCED_COMMAND_STREAM = 1;

    /** Sequenced event stream (StreamId 2) — read by EventHandlers and ReadModels. */
    public static final int SEQUENCED_EVENT_STREAM = 2;

    private OmsStreams() {}
}
