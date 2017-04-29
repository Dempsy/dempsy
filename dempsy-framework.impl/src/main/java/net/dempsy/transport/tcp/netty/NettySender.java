package net.dempsy.transport.tcp.netty;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import net.dempsy.DempsyException;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.util.io.MessageBufferOutput;

public final class NettySender implements Sender {
    private final static ConcurrentLinkedQueue<MessageBufferOutput> pool = new ConcurrentLinkedQueue<>();

    private final static Logger LOGGER = LoggerFactory.getLogger(NettySender.class);
    private static final AtomicLong threadNum = new AtomicLong(0);

    private final NodeStatsCollector statsCollector;
    private final TcpAddress addr;
    private final Serializer serializer;
    private final NettySenderFactory owner;
    private final AtomicReference<Internal> connection = new AtomicReference<>(null);
    private final EventLoopGroup group;

    private boolean isRunning = true;

    NettySender(final TcpAddress addr, final NettySenderFactory parent, final NodeStatsCollector statsCollector,
            final Manager<Serializer> serializerManger, final EventLoopGroup group) {
        this.addr = addr;
        serializer = serializerManger.getAssociatedInstance(addr.serializerId);
        this.statsCollector = statsCollector;
        this.owner = parent;
        this.group = group;
        reset();
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        try {
            final Internal cur = connection.get();
            if (cur != null) {
                connection.get().ch.writeAndFlush(message).sync();
            }
        } catch (final InterruptedException e) {
            throw new MessageTransportException(e);
        }
    }

    @Override
    public synchronized void stop() {
        isRunning = false;
        reset();
        owner.imDone(addr);
    }

    private void reset() {
        final Internal previous = connection.getAndSet(isRunning ? new Internal() : null);

        if (previous != null)
            previous.stop();
    }

    private MessageBufferOutput getPooled() {
        final MessageBufferOutput tmp = pool.poll();
        if (tmp != null) {
            tmp.reset();
            return tmp;
        } else {
            return new MessageBufferOutput();
        }

    }

    private static class MyByteBuf extends UnpooledHeapByteBuf {
        final MessageBufferOutput toRelease;

        MyByteBuf(final ByteBufAllocator alloc, final MessageBufferOutput mbo) {
            super(alloc, mbo.getBuffer(), mbo.getBuffer().length);
            this.toRelease = mbo;
            writerIndex(mbo.getPosition());
        }

        @Override
        protected void deallocate() {
            // hook me.
            super.deallocate();
            pool.offer(toRelease);
        }
    }

    private class Internal {
        Channel ch = null;

        Internal() {
            reset();
        }

        synchronized void reset() {
            try {
                final Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        // .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(false))
                        .handler(new ChannelInitializer<SocketChannel>() {

                            @Override
                            protected void initChannel(final SocketChannel ch) throws Exception {
                                final ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast(new MessageToMessageEncoder<Object>() {

                                    @Override
                                    protected void encode(final ChannelHandlerContext ctx, final Object msg, final List<Object> out)
                                            throws Exception {

                                        final MessageBufferOutput o = getPooled();
                                        serializer.serialize(msg, o);
                                        ByteBuf preamble;
                                        if (o.getPosition() > Short.MAX_VALUE) {
                                            try (final MessageBufferOutput preamblembo = new MessageBufferOutput(6);) {
                                                preamblembo.writeShort((short) -1);
                                                preamblembo.writeInt(o.getPosition());
                                                preamble = Unpooled.wrappedBuffer(preamblembo.getBuffer());
                                            }
                                        } else {
                                            try (final MessageBufferOutput preamblembo = new MessageBufferOutput(2);) {
                                                preamblembo.writeShort((short) o.getPosition());
                                                preamble = Unpooled.wrappedBuffer(preamblembo.getBuffer());
                                            }
                                        }
                                        out.add(preamble);
                                        out.add(new MyByteBuf(ctx.alloc(), o));

                                        if (statsCollector != null)
                                            statsCollector.messageSent(msg);
                                    }

                                    @Override
                                    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
                                        LOGGER.error("Failed writing to {}", addr, cause);
                                        // TODO: pass the failed message over for another try.
                                        NettySender.this.reset();
                                    }
                                });
                            }
                        });

                // Start the connection attempt.
                ch = b.connect(addr.inetAddress, addr.port).sync().channel();

            } catch (final InterruptedException ie) {
                throw new DempsyException(ie);
            } catch (final Exception ioe) { // Netty can throw an IOException here, but it uses UNSAFE to do it
                                            // so we can't catch it because the 'connect' doesn't declare that
                                            // it throws it. Stupid f-tards!
                if (RuntimeException.class.isAssignableFrom(ioe.getClass())) {
                    throw (RuntimeException) ioe;
                } else {
                    LOGGER.warn("Unexpected and undeclared Netty excpetion {}", ioe.getLocalizedMessage());
                    throw new DempsyException(ioe);
                }
            }
        }

        // REALLY REALLY STOP!
        synchronized void stop() {
            final Channel tmpCh = ch; // in case close throws.
            ch = null;
            try {
                if (tmpCh != null) {
                    if (!tmpCh.close().await(1000)) {
                        LOGGER.warn("Had an issue closing the sender socket.");
                        startCloseThread(tmpCh,
                                "netty-sender-closer" + threadNum.getAndIncrement() + "-to(" + addr.getGuid() + ") from (" + owner.nodeId + ")");
                    }
                }
            } catch (final Exception e) {
                LOGGER.warn("Unexpected exception closing sender socket connection", e);
                startCloseThread(tmpCh,
                        "netty-sender-closer" + threadNum.getAndIncrement() + "-to(" + addr.getGuid() + ") from (" + owner.nodeId + ")");
                return;
            }

        }
    }

    private static void startCloseThread(final Channel tmpCh, final String threadName) {
        // close the damn thing in another thread insistently
        new Thread(() -> {
            while (tmpCh.isOpen()) {
                try {
                    if (!tmpCh.close().await(1000))
                        LOGGER.warn("Had an issue closing the sender socket.");
                } catch (final Exception ee) {
                    LOGGER.warn("Had an issue closing the sender socket.", ee);
                }
            }
        }, threadName).start();
    }
}