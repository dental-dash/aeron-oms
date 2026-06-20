package com.oms.fix.client;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of pending SSE emitters keyed by {@code clOrdId}.
 *
 * <p>An emitter is registered when the REST controller accepts an order request
 * and removed when the execution report arrives or the emitter times out.
 */
@Component
public class EmitterRegistry
{
    private final ConcurrentHashMap<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    public void register(final String clOrdId, final SseEmitter emitter)
    {
        emitters.put(clOrdId, emitter);
    }

    public SseEmitter get(final String clOrdId)
    {
        return emitters.get(clOrdId);
    }

    public void remove(final String clOrdId)
    {
        emitters.remove(clOrdId);
    }
}
