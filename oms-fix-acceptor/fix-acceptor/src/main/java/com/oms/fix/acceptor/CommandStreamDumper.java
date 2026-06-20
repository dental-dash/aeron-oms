//package com.oms.fix.acceptor;
//
//import com.oms.common.OmsStreams;
//import com.oms.fix.sbe.Decimal64Decoder;
//import com.oms.fix.sbe.MessageHeaderDecoder;
//import com.oms.fix.sbe.FixNewOrderSingleCommandDecoder;
//import io.aeron.Aeron;
//import io.aeron.Subscription;
//import io.aeron.logbuffer.FragmentHandler;
//
//import java.nio.file.Paths;
//
///**
// * M4: Standalone verification subscriber — no Artio, pure Aeron + SBE.
// * Connects to the acceptor's ArchivingMediaDriver (./aeron-fix-acceptor),
// * subscribes to IPC stream 1 (Sequenced Command Stream), and prints each
// * NewOrderSingleCommand including the sequenceNumber stamped by FixSequencerAgent.
// *
// * <p>Run after the acceptor is up. Send orders via the FIX client; verify that
// * sequenceNumbers start at 1 and increment with no gaps.
// */
//public final class CommandStreamDumper
//{
//    public static void main(final String[] args) throws Exception
//    {
//        // Must match FixAcceptorMain.AERON_DIR — the acceptor's driver lives there, not in the default dir.
//        final String aeronDirAbsolute = Paths.get("./aeron-fix-acceptor").toAbsolutePath().normalize().toString();
//        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirAbsolute));
//        // M4: subscribe to stream 1 (Sequenced Command Stream) — sequencer stamps sequence numbers here.
//        // Previously stream 10 (raw ingress); changed for M4 to verify sequencer pass-through.
//        final Subscription sub = aeron.addSubscription(OmsStreams.IPC, OmsStreams.COMMAND_STREAM);
//
//        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
//        final FixNewOrderSingleCommandDecoder nosDecoder = new FixNewOrderSingleCommandDecoder();
//
//        final FragmentHandler handler = (buffer, offset, length, header) ->
//        {
//            headerDecoder.wrap(buffer, offset);
//
//            if (headerDecoder.templateId() != FixNewOrderSingleCommandDecoder.TEMPLATE_ID)
//            {
//                System.out.println("[Dumper] Unknown templateId=" + headerDecoder.templateId() + " — skipping");
//                return;
//            }
//
//            nosDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
//
//            final Decimal64Decoder price    = nosDecoder.price();
//            final Decimal64Decoder orderQty = nosDecoder.orderQty();
//
//            System.out.printf(
//                "[Dumper] NOS seqNum=%d sessionId=%d clOrdId=%s symbol=%s side=%s qty=%de%d price=%de%d%n",
//                nosDecoder.sequenceNumber(),
//                nosDecoder.sessionId(),
//                nosDecoder.clOrdId(),
//                nosDecoder.symbol(),
//                nosDecoder.side(),
//                orderQty.mantissa(), orderQty.exponent(),
//                price.mantissa(),    price.exponent());
//        };
//
//        Runtime.getRuntime().addShutdownHook(new Thread(() ->
//        {
//            sub.close();
//            aeron.close();
//        }));
//
//        System.out.println("[Dumper] Listening on aeron:ipc stream " + OmsStreams.COMMAND_STREAM +
//            " (Sequenced Command Stream) — verifying sequence numbers...");
//        while (!Thread.currentThread().isInterrupted())
//        {
//            sub.poll(handler, 10);
//            Thread.sleep(1);
//        }
//    }
//}
