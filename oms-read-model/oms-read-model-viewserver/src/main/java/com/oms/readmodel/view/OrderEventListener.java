package com.oms.readmodel.view;

/**
 * Fired by ViewServerReadModel after each order insert/update.
 * Implementations MUST be non-blocking — called from the ViewServer AgentRunner thread.
 * Undertow's WebSockets.sendText() is thread-safe and non-blocking — safe to call here.
 * TODO(POC): add back-pressure handling if WS write buffers fill under load.
 */
public interface OrderEventListener {
    void onOrderUpdate(long orderId, OrderView view);
}
