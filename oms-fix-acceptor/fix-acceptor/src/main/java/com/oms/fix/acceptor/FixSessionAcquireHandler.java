package com.oms.fix.acceptor;

import uk.co.real_logic.artio.library.SessionAcquireHandler;
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

import java.util.concurrent.ConcurrentHashMap;

/**
 * M3: creates a {@link FixSessionHandler} per session, wired to the Aeron command-stream publisher.
 * M7: passes shared {@code activeSessions} map so the handler can register/deregister sessions.
 */
public class FixSessionAcquireHandler implements SessionAcquireHandler
{
    private final CommandStreamPublisher publisher;
    private final ConcurrentHashMap<Long, Session> activeSessions;

    public FixSessionAcquireHandler(final CommandStreamPublisher publisher,
                                    final ConcurrentHashMap<Long, Session> activeSessions)
    {
        this.publisher       = publisher;
        this.activeSessions  = activeSessions;
    }

    @Override
    public SessionHandler onSessionAcquired(final Session session, final SessionAcquiredInfo acquiredInfo)
    {
        final CompositeKey key = session.compositeKey();
        // Register here — before any onMessage() calls — so ExecReportBridge can find the session
        // as soon as the OrderAcceptedEvent arrives. onSessionStart() fires later (after Logon ack)
        // and would be too late if a NOS is processed and accepted synchronously on login.
        activeSessions.put(session.id(), session);
        System.out.printf("[Acceptor] Session acquired: id=%d local=%s remote=%s (registered in activeSessions)%n",
            session.id(), key.localCompId(), key.remoteCompId());
        return new FixSessionHandler(publisher, activeSessions);
    }
}
