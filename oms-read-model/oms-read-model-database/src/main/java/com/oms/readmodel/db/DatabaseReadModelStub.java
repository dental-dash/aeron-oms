package com.oms.readmodel.db;

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
 * Stub DatabaseReadModel — subscribes to the Event Stream (StreamId 2) and logs each event.
 *
 * TODO(POC): replace with H2/JDBC projection in Milestone 2. Track lastProcessedSequence
 *            for checkpoint-based Archive replay on restart (Milestone 4).
 */
public class DatabaseReadModelStub implements Agent {

    private static final Log log = LogFactory.getLog(DatabaseReadModelStub.class);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private final Subscription eventStreamSub;
    private final FragmentHandler fragmentHandler;

    public DatabaseReadModelStub(Subscription eventStreamSub) {
        this.eventStreamSub  = eventStreamSub;
        this.fragmentHandler = this::onFragment;
    }

    @Override
    public int doWork() {
        return eventStreamSub.poll(fragmentHandler, 10);
    }

    @Override
    public String roleName() { return "db-read-model"; }

    @Override
    public void onStart() {
        log.info().append("db-read-model started").commit();
    }

    @Override
    public void onClose() {
        log.info().append("db-read-model closed").commit();
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();
        long seq = buffer.getLong(
                offset + MessageHeaderDecoder.ENCODED_LENGTH, ByteOrder.LITTLE_ENDIAN);

        log.info()
            .append("[db-read-model] event templateId=").append(templateId)
            .append(" seq=").append(seq)
            .commit();
        // TODO(POC): track lastProcessedSequence = seq.
        //            On restart, replay Event Stream from Archive at checkpoint position
        //            (same pattern as ViewServerReadModel). Implement when real DB upsert is added.
    }
}
