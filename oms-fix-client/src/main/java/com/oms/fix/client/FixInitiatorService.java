package com.oms.fix.client;

import com.oms.fix.client.dto.CancelOrderRequest;
import com.oms.fix.client.dto.CancelReplaceRequest;
import com.oms.fix.client.dto.OrderType;
import com.oms.fix.client.dto.PlaceOrderRequest;
import com.oms.fix.client.dto.Side;
import io.aeron.archive.ArchivingMediaDriver;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import uk.co.real_logic.artio.builder.OrderCancelReplaceRequestEncoder;
import uk.co.real_logic.artio.builder.OrderCancelRequestEncoder;
import uk.co.real_logic.artio.builder.NewOrderSingleEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.Reply;

import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * M9: Owns the Artio {@link FixEngine} + {@link FixLibrary} lifecycle and the dedicated
 * poll thread. REST controllers call {@code submitNos / submitCancel / submitCancelReplace}
 * which enqueue a send task; the poll thread drains the queue each iteration.
 *
 * <p>Implements {@link SmartLifecycle} so Spring manages start/stop ordering.
 * The {@link ArchivingMediaDriver} bean is destroyed by Spring AFTER this service
 * has stopped — ensuring orderly shutdown.
 */
@Service
public class FixInitiatorService implements SmartLifecycle
{
    private static final String LIBRARY_CHANNEL = "aeron:ipc";
    private static final String ACCEPTOR_HOST   = "localhost";
    private static final int    ACCEPTOR_PORT   = 9880;
    private static final String LOG_DIR         = "./fix-client-logs";
    private static final String ARCHIVE_CONTROL = "aeron:udp?endpoint=localhost:8020";
    // Fixed port avoids ephemeral-port registration race (see architecture notes).
    // TODO(POC): migrate to aeron:ipc response channel if port-reuse becomes an issue.
    private static final String ARCHIVE_RESPONSE = "aeron:udp?endpoint=localhost:8021";
    private static final String SENDER_COMP_ID  = "CLIENT";
    private static final String TARGET_COMP_ID  = "ACCEPTOR";

    private final ArchivingMediaDriver    artioDriver;
    private final FixClientSessionHandler sessionHandler;
    private final EmitterRegistry         emitterRegistry;

    private FixEngine engine;
    private FixLibrary library;
    volatile Session session;              // written on poll thread, read on HTTP threads

    private Thread           pollThread;
    private volatile boolean running;
    private final Queue<Runnable> sendQueue = new ConcurrentLinkedQueue<>();

    // Pre-allocated encoders — used ONLY on the poll thread (not thread-safe).
    private final NewOrderSingleEncoder             nos             = new NewOrderSingleEncoder();
    private final OrderCancelRequestEncoder         cancelReq       = new OrderCancelRequestEncoder();
    private final OrderCancelReplaceRequestEncoder  cancelReplaceReq = new OrderCancelReplaceRequestEncoder();
    private final UtcTimestampEncoder               tsEncoder       = new UtcTimestampEncoder();

    public FixInitiatorService(final ArchivingMediaDriver artioDriver,
                               final FixClientSessionHandler sessionHandler,
                               final EmitterRegistry emitterRegistry)
    {
        this.artioDriver    = artioDriver;
        this.sessionHandler = sessionHandler;
        this.emitterRegistry = emitterRegistry;
    }

    // ── SmartLifecycle ───────────────────────────────────────────────────────

    @Override
    public void start()
    {
        final String aeronDir = System.getProperty("aeron.dir");

        final EngineConfiguration engineCfg = new EngineConfiguration()
            .libraryAeronChannel(LIBRARY_CHANNEL)
            .logFileDir(LOG_DIR)
            .deleteLogFileDirOnStart(true)
            .scheduler(new LowResourceEngineScheduler());

        engineCfg.aeronArchiveContext()
            .controlRequestChannel(ARCHIVE_CONTROL)
            .controlResponseChannel(ARCHIVE_RESPONSE)
            .aeronDirectoryName(aeronDir);

        engine = FixEngine.launch(engineCfg);

        final LibraryConfiguration libraryCfg = new LibraryConfiguration()
            .sessionAcquireHandler(sessionHandler)
            .sessionExistsHandler(
                (library, surrogateId, localCompId, localSubId, localLocationId,
                 remoteCompId, remoteSubId, remoteLocationId, logonSeqNum, seqIndex) ->
                    System.out.printf("[Client] Existing session: local=%s remote=%s%n",
                        localCompId, remoteCompId))
            .libraryAeronChannels(Collections.singletonList(LIBRARY_CHANNEL));

        library = FixLibrary.connect(libraryCfg);

        // Warm-up: let the engine's Framer register this library before initiating.
        warmUp(50, 10);

        final SessionConfiguration sessionCfg = SessionConfiguration.builder()
            .address(ACCEPTOR_HOST, ACCEPTOR_PORT)
            .senderCompId(SENDER_COMP_ID)
            .targetCompId(TARGET_COMP_ID)
            // Reset sequence numbers on restart so the acceptor doesn't reject as OOS.
            // TODO(POC): remove for persistent sequence tracking.
            .resetSeqNum(true)
            .build();

        System.out.printf("[Client] Initiating FIX session → %s:%d%n", ACCEPTOR_HOST, ACCEPTOR_PORT);
        final Reply<Session> reply = library.initiate(sessionCfg);

        // Poll until session is established (or timeout after 10 s).
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (reply.isExecuting() && System.currentTimeMillis() < deadline)
        {
            library.poll(10);
            sleepMs(1);
        }

        if (reply.hasCompleted())
        {
            System.out.printf("[Client] Session initiated: id=%d%n",
                reply.resultIfPresent() != null ? reply.resultIfPresent().id() : -1L);
        }
        else
        {
            System.err.printf("[Client] WARN session initiation not complete yet (state=%s) — will complete on poll thread%n",
                reply.state());
        }

        running = true;
        pollThread = new Thread(this::pollLoop, "fix-client-poll");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    @Override
    public void stop()
    {
        running = false;
        if (pollThread != null)
        {
            pollThread.interrupt();
            try { pollThread.join(5_000L); }
            catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (library != null) { library.close(); }
        if (engine  != null) { engine.close();  }
        // artioDriver is closed by Spring (destroyMethod = "close") after this returns.
    }

    @Override
    public boolean isRunning() { return running; }

    // ── Public API — called from HTTP threads ────────────────────────────────

    public SseEmitter submitNos(final PlaceOrderRequest req)
    {
        final String clOrdId = nextClOrdId();
        final SseEmitter emitter = createEmitter(clOrdId);
        sendQueue.offer(() -> sendNos(clOrdId, req, emitter));
        return emitter;
    }

    public SseEmitter submitCancel(final CancelOrderRequest req)
    {
        final String clOrdId = nextClOrdId();
        final SseEmitter emitter = createEmitter(clOrdId);
        sendQueue.offer(() -> sendCancel(clOrdId, req, emitter));
        return emitter;
    }

    public SseEmitter submitCancelReplace(final CancelReplaceRequest req)
    {
        final String clOrdId = nextClOrdId();
        final SseEmitter emitter = createEmitter(clOrdId);
        sendQueue.offer(() -> sendCancelReplace(clOrdId, req, emitter));
        return emitter;
    }

    /** Called by {@link FixClientSessionHandler.LoggingSessionHandler#onSessionStart} on the poll thread. */
    void onSessionReady(final Session s)
    {
        this.session = s;
        System.out.printf("[Client] Session ready: id=%d%n", s.id());
    }

    // ── Poll loop ────────────────────────────────────────────────────────────

    private void pollLoop()
    {
        while (running && !Thread.currentThread().isInterrupted())
        {
            library.poll(10);

            // Drain the send queue on the poll thread — Artio session writes are not thread-safe.
            Runnable task;
            while ((task = sendQueue.poll()) != null)
            {
                task.run();
            }

            sleepMs(1);
        }
    }

    // ── Send helpers — executed only on poll thread ──────────────────────────

    private void sendNos(final String clOrdId, final PlaceOrderRequest req, final SseEmitter emitter)
    {
        if (session == null)
        {
            completeWithSessionError(clOrdId, emitter);
            return;
        }

        final char fixSide    = req.side() == Side.BUY ? '1' : '2';
        final char fixOrdType = req.orderType() == OrderType.LIMIT ? '2' : '1';

        nos.reset();
        nos.clOrdID(clOrdId);
        nos.instrument().symbol(req.symbol());
        nos.side(fixSide);
        nos.ordType(fixOrdType);
        nos.price(req.price().unscaledValue().longValue(), -req.price().scale());
        nos.orderQtyData().orderQty(req.quantity().unscaledValue().longValue(), -req.quantity().scale());
        final int tsLen = tsEncoder.encode(System.currentTimeMillis());
        nos.transactTime(tsEncoder.buffer(), 0, tsLen);

        final long result = session.trySend(nos);
        System.out.printf("[Client] Sent NOS clOrdId=%s trySend=%d%n", clOrdId, result);
        if (result < 0)
        {
            // TODO(POC): back-pressure — log and complete with error; retry queue in production.
            System.err.printf("[Client] NOS back-pressure: result=%d clOrdId=%s%n", result, clOrdId);
        }
    }

    private void sendCancel(final String clOrdId, final CancelOrderRequest req, final SseEmitter emitter)
    {
        if (session == null)
        {
            completeWithSessionError(clOrdId, emitter);
            return;
        }

        final char fixSide = req.side() == Side.BUY ? '1' : '2';

        cancelReq.reset();
        cancelReq.clOrdID(clOrdId);
        cancelReq.origClOrdID(req.origClOrdId());
        cancelReq.instrument().symbol(req.symbol());
        cancelReq.side(fixSide);
        final int tsLen = tsEncoder.encode(System.currentTimeMillis());
        cancelReq.transactTime(tsEncoder.buffer(), 0, tsLen);

        final long result = session.trySend(cancelReq);
        System.out.printf("[Client] Sent CancelReq clOrdId=%s origClOrdId=%s trySend=%d%n",
            clOrdId, req.origClOrdId(), result);
    }

    private void sendCancelReplace(final String clOrdId, final CancelReplaceRequest req,
                                   final SseEmitter emitter)
    {
        if (session == null)
        {
            completeWithSessionError(clOrdId, emitter);
            return;
        }

        final char fixSide    = req.side() == Side.BUY ? '1' : '2';
        final char fixOrdType = req.orderType() == OrderType.LIMIT ? '2' : '1';

        cancelReplaceReq.reset();
        cancelReplaceReq.clOrdID(clOrdId);
        cancelReplaceReq.origClOrdID(req.origClOrdId());
        cancelReplaceReq.instrument().symbol(req.symbol());
        cancelReplaceReq.side(fixSide);
        cancelReplaceReq.ordType(fixOrdType);
        cancelReplaceReq.price(req.price().unscaledValue().longValue(), -req.price().scale());
        cancelReplaceReq.orderQtyData().orderQty(req.quantity().unscaledValue().longValue(), -req.quantity().scale());
        final int tsLen = tsEncoder.encode(System.currentTimeMillis());
        cancelReplaceReq.transactTime(tsEncoder.buffer(), 0, tsLen);

        final long result = session.trySend(cancelReplaceReq);
        System.out.printf("[Client] Sent CancelReplaceReq clOrdId=%s origClOrdId=%s trySend=%d%n",
            clOrdId, req.origClOrdId(), result);
    }

    // ── Utilities ────────────────────────────────────────────────────────────

    private SseEmitter createEmitter(final String clOrdId)
    {
        final SseEmitter emitter = new SseEmitter(30_000L);
        emitterRegistry.register(clOrdId, emitter);
        emitter.onCompletion(() -> emitterRegistry.remove(clOrdId));
        emitter.onTimeout(() ->
        {
            emitterRegistry.remove(clOrdId);
            emitter.complete();
        });
        return emitter;
    }

    private void completeWithSessionError(final String clOrdId, final SseEmitter emitter)
    {
        try
        {
            emitter.send(SseEmitter.event().name("error").data("FIX session not available"));
            emitter.complete();
        }
        catch (final Exception ignored) {}
        emitterRegistry.remove(clOrdId);
    }

    private static String nextClOrdId()
    {
        // Short UUID suffix keeps clOrdId well under the 36-char SBE field limit.
        return "ORD-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
    }

    private void warmUp(final int iterations, final long sleepMs)
    {
        for (int i = 0; i < iterations; i++)
        {
            library.poll(10);
            sleepMs(sleepMs);
        }
    }

    private static void sleepMs(final long ms)
    {
        try { Thread.sleep(ms); }
        catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
