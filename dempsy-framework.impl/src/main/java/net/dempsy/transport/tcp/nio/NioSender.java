package net.dempsy.transport.tcp.nio;

import static net.dempsy.transport.tcp.nio.internal.NioUtils.dontInterrupt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.transport.tcp.nio.internal.NioUtils;

public final class NioSender implements Sender {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioSender.class);

    final TcpAddress addr;
    final String nodeId;

    private final NioSenderFactory owner;
    private final NodeStatsCollector statsCollector;

    public final Serializer serializer;

    SocketChannel channel = null;

    private boolean connected = false;
    private int sendBufferSize = -1;
    private int recvBufferSize = -1;

    // read from Sending
    BlockingQueue<Object> messages;
    boolean running = true;

    NioSender(final TcpAddress addr, final NioSenderFactory parent) {
        this.owner = parent;
        this.addr = addr;
        serializer = parent.serializerManager.getAssociatedInstance(addr.serializerId);
        this.statsCollector = parent.statsCollector;
        this.nodeId = parent.nodeId;

        // messages = new LinkedBlockingQueue<>();
        messages = new ArrayBlockingQueue<>(2);
        makeChannel();
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        boolean done = false;
        while (running && !done) {
            if (running) {
                try {
                    // let's not try forever in case we're locked up and shutting down.
                    done = messages.offer(message, 1, TimeUnit.SECONDS);
                } catch (final InterruptedException ie) {}
            }
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        dontInterrupt(() -> Thread.sleep(1));

        final List<Object> drainTo = new ArrayList<>();

        // in case we're being bombarded with sends from another thread,
        // we'll keep trying this until everyone realizes we're stopped.
        boolean stillNotDone = true;
        final long stillNotDoneStartTime = System.currentTimeMillis();
        while (stillNotDone) {
            for (boolean doneGettingStopMessageQueued = false; !doneGettingStopMessageQueued;) {
                messages.drainTo(drainTo);
                doneGettingStopMessageQueued = messages.offer(new StopMessage());
            }
            final long startTime = System.currentTimeMillis();
            while (stillNotDone) {
                if (!channel.isOpen() && channel.socket().isClosed())
                    stillNotDone = false;
                else if ((System.currentTimeMillis() - startTime) > 500)
                    break;
                else
                    dontInterrupt(() -> Thread.sleep(1));
            }

            // if X seconds have passed let's just close it from this side and move on.
            if ((System.currentTimeMillis() - stillNotDoneStartTime) > 3000) {
                stillNotDone = false;
                NioUtils.closeQuietly(channel, LOGGER, nodeId + " failed directly closing channel from " + NioSender.class);
                if (!channel.socket().isClosed())
                    NioUtils.closeQuietly(channel.socket(), LOGGER, nodeId + " failed directly closing socket from " + NioSender.class);
            }
        }

        drainTo.forEach(o -> statsCollector.messageNotSent());
        owner.idleSenders.remove(this);
        owner.imDone(addr);
    }

    static class StopMessage {}

    void connect(final boolean force) throws IOException {
        if (!connected || force) {
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(addr.inetAddress, addr.port));
            while (!channel.finishConnect())
                Thread.yield();
            sendBufferSize = channel.socket().getSendBufferSize();
            recvBufferSize = addr.recvBufferSize;
            connected = true;
            owner.idleSenders.putIfAbsent(this, this);
        }
    }

    SocketChannel makeChannel() {
        final SocketChannel ret = channel;
        try {
            channel = SocketChannel.open();
        } catch (final IOException e) {
            throw new MessageTransportException(e); // this is probably impossible
        }
        return ret;
    }

    private int cachedBatchSize = -1;

    int getMaxBatchSize() {
        if (cachedBatchSize < 0) {
            int ret;
            if (recvBufferSize <= 0)
                ret = sendBufferSize;
            else if (sendBufferSize <= 0)
                ret = recvBufferSize;
            else ret = Math.min(recvBufferSize, sendBufferSize);
            if (ret <= 0) {
                LOGGER.warn(nodeId + " sender to " + addr.toString() + " couldn't determine send and receieve buffer sizes. Setting batch size to ");
                ret = owner.mtu;
            }
            cachedBatchSize = Math.min(ret, owner.mtu);
        }
        return cachedBatchSize;
    }
}