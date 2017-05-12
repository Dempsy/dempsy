package net.dempsy.transport.tcp.nio.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;

import net.dempsy.util.Functional.RunnableThrows;
import net.dempsy.util.io.MessageBufferOutput;

public class NioUtils {
    // =============================================================================
    // These classes manage the buffer pool used by the readers and clients
    // =============================================================================
    private static ConcurrentLinkedQueue<ReturnableBufferOutput> bufferPool = new ConcurrentLinkedQueue<>();

    public static ReturnableBufferOutput get() {
        ReturnableBufferOutput ret = bufferPool.poll();
        if (ret == null)
            ret = new ReturnableBufferOutput();
        return ret;
    }

    public static boolean closeQuietly(final AutoCloseable ac, final Logger LOGGER, final String failedMessage) {
        try {
            ac.close();
            return true;
        } catch (final Exception e) {
            LOGGER.warn(failedMessage, e);
        }
        return false;
    }

    public static void dontInterrupt(final RunnableThrows<InterruptedException> runner) {
        try {
            runner.run();
        } catch (final InterruptedException ie) {}
    }

    public static void dontInterrupt(final RunnableThrows<InterruptedException> runner, final Consumer<InterruptedException> handler) {
        try {
            runner.run();
        } catch (final InterruptedException ie) {
            handler.accept(ie);
        }
    }

    public static class ReturnableBufferOutput extends MessageBufferOutput {
        private ByteBuffer bb = null;
        private boolean flopped = false;

        public int messageStart = -1;

        private ReturnableBufferOutput() {
            super(2048); /// holds at least one full packet
        }

        public ByteBuffer getBb() {
            if (bb == null)
                bb = ByteBuffer.wrap(getBuffer());
            return bb;
        }

        public ByteBuffer getFloppedBb() {
            final ByteBuffer lbb = getBb();
            if (!flopped) {
                lbb.limit(getPosition());
                lbb.position(0); // position to zero.
                flopped = true;
            }
            return lbb;
        }

        @Override
        public void close() {
            super.close();
            reset();
            messageStart = -1;
            bb = null;
            flopped = false;
            bufferPool.offer(this);
        }

        @Override
        public void grow(final int newcap) {
            super.grow(newcap);
            if (bb != null) {
                final ByteBuffer obb = bb;
                bb = ByteBuffer.wrap(getBuffer());
                bb.position(obb.position());
                bb.limit(obb.limit());
                flopped = false;
            }
        }
    }

}
