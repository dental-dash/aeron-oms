package com.oms.readmodel.view;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.oms.sbe.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

import java.nio.ByteOrder;

/**
 * Stub ViewServerReadModel — subscribes to the Event Stream (StreamId 2) and logs each event.
 *
 * TODO(POC): replace with in-memory ConcurrentHashMap<OrderId, OrderView> projection in
 *            Milestone 2. Expose via WebSocket push in Milestone 5.
 */
public class ViewServerReadModelStub implements Agent {

    private static final Log log = LogFactory.getLog(ViewServerReadModelStub.class);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private final Subscription eventStreamSub;
    private final FragmentHandler fragmentHandler;

    public ViewServerReadModelStub(Subscription eventStreamSub) {
        this.eventStreamSub  = eventStreamSub;
        this.fragmentHandler = this::onFragment;
    }

    @Override
    public int doWork() {
        return eventStreamSub.poll(fragmentHandler, 10);
    }

    @Override
    public String roleName() { return "view-server"; }

    @Override
    public void onStart() {
        log.info().append("view-server started").commit();
    }

    @Override
    public void onClose() {
        log.info().append("view-server closed").commit();
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();
        long seq = buffer.getLong(
                offset + MessageHeaderDecoder.ENCODED_LENGTH, ByteOrder.LITTLE_ENDIAN);

        log.info()
            .append("[view-server] event templateId=").append(templateId)
            .append(" seq=").append(seq)
            .commit();
    }
}
