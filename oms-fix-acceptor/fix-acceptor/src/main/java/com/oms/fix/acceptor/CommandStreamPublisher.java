package com.oms.fix.acceptor;

import io.aeron.Publication;
import org.agrona.DirectBuffer;

/**
 * Wraps an Aeron {@link Publication} with bounded spin-retry.
 * NOT_CONNECTED / BACK_PRESSURED → spin up to MAX_OFFER_ATTEMPTS then drop.
 * CLOSED → fatal; throws immediately.
 */
public final class CommandStreamPublisher
{
    private static final int MAX_OFFER_ATTEMPTS = 100;
    private final Publication publication;

    public CommandStreamPublisher(final Publication publication)
    {
        this.publication = publication;
    }

    public void offer(final DirectBuffer buffer, final int offset, final int length)
    {
        long lastResult = 0;
        for (int attempt = 0; attempt < MAX_OFFER_ATTEMPTS; attempt++)
        {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0)
            {
                return;
            }
            if (result == Publication.CLOSED)
            {
                throw new IllegalStateException("Publication is CLOSED — cannot publish command");
            }
            lastResult = result;
            // NOT_CONNECTED or BACK_PRESSURED: spin
        }
        // TODO(POC): replace with structured logging
        final String reason = lastResult == Publication.NOT_CONNECTED ? "NOT_CONNECTED (is OmsApp running?)"
            : lastResult == Publication.BACK_PRESSURED ? "BACK_PRESSURED"
            : "result=" + lastResult;
        System.err.printf("[CommandStream] Drop after %d attempts — %s%n", MAX_OFFER_ATTEMPTS, reason);
    }
}
