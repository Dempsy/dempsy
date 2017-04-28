package net.dempsy.transport.tcp.netty;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import net.dempsy.DempsyException;
import net.dempsy.Router.RoutedMessage;
import net.dempsy.serialization.Serializer;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.transport.tcp.TcpAddressResolver;
import net.dempsy.transport.tcp.TcpUtils;
import net.dempsy.util.io.MessageBufferInput;

public class NettyReceiver<T> implements Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyReceiver.class);
    private TcpAddressResolver resolver = a -> a;
    private TcpAddress address = null;
    private TcpAddress internal = null;
    private boolean useLocalHost = false;
    private int internalPort = -1;
    private int numHandlers = 1;
    private EventLoopGroup parentGroup = null;
    private EventLoopGroup childGroup = null;
    private Listener<T> typedListener = null;

    private final Serializer serializer;

    public NettyReceiver(final Serializer serializer, final int port) {
        this.internalPort = port;
        this.serializer = serializer;
    }

    public NettyReceiver(final Serializer serializer) {
        this(serializer, -1);
    }

    public NettyReceiver<T> setUseLocalHost(final boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
        return this;
    }

    public NettyReceiver<T> setNumHandlers(final int numHandlers) {
        this.numHandlers = numHandlers;
        return this;
    }

    public NettyReceiver<T> setResolver(final TcpAddressResolver resolver) {
        this.resolver = resolver;
        return this;
    }

    @Override
    public synchronized void close() throws Exception {
        final Future<?> cf = (childGroup != null) ? childGroup.shutdownGracefully() : null;
        final Future<?> pf = (parentGroup != null) ? parentGroup.shutdownGracefully() : null;
        if (cf != null)
            cf.await();
        if (pf != null)
            pf.await();
    }

    @Override
    public synchronized TcpAddress getAddress() {
        if (internal == null) {
            try {
                final InetAddress addr = useLocalHost ? Inet4Address.getLocalHost() : TcpUtils.getFirstNonLocalhostInetAddress();
                final InetSocketAddress inetSocketAddress = doBind(addr, (internalPort < 0) ? 0 : internalPort);
                internalPort = inetSocketAddress.getPort();
                final String serId = serializer.getClass().getPackage().getName();
                internal = new TcpAddress(addr, internalPort, serId);
                address = resolver.getExternalAddresses(internal);
            } catch (final IOException e) {
                throw new DempsyException(e);
            }
        }
        return address;
    }

    private static AtomicLong acceptorThreadNum = new AtomicLong(0L);
    private static AtomicLong globalHandlerGroupNum = new AtomicLong(0L);

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void start(final Listener<?> listener, final ThreadingModel threadingModel) throws MessageTransportException {
        this.typedListener = (Listener<T>) listener;
    }

    private InetSocketAddress doBind(final InetAddress inetAddress, final int port) {
        final ServerBootstrap b = new ServerBootstrap();
        parentGroup = new NioEventLoopGroup(1, (ThreadFactory) r -> new Thread(r,
                NettyReceiver.class.getSimpleName() + "-Acceptor-" + acceptorThreadNum.getAndIncrement()));
        final AtomicLong handlerNum = new AtomicLong(0);
        final long ghgn = globalHandlerGroupNum.getAndIncrement();
        childGroup = new NioEventLoopGroup(numHandlers, (ThreadFactory) r -> new Thread(r,
                NettyReceiver.class.getSimpleName() + "-Handler(" + ghgn + ")-" + handlerNum.getAndIncrement()));

        b.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                // .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(false))
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(final SocketChannel ch) throws Exception {
                        // ch.config().setAllocator(new PooledByteBufAllocator(false) {
                        // @Override
                        // public ByteBuf ioBuffer() {
                        // return heapBuffer();
                        // }
                        //
                        // @Override
                        // public ByteBuf ioBuffer(final int initialCapacity) {
                        // return heapBuffer(initialCapacity);
                        // }
                        //
                        // @Override
                        // public ByteBuf ioBuffer(final int initialCapacity, final int maxCapacity) {
                        // return heapBuffer(initialCapacity, maxCapacity);
                        // }
                        //
                        // });
                        ch.pipeline().addLast(new Client<T>(serializer, () -> typedListener));
                    }

                });

        final InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, port);
        final InetSocketAddress localAddr;
        // wait until the bind is complete.
        try {
            localAddr = (InetSocketAddress) b.bind(inetSocketAddress).sync().await().channel().localAddress();
        } catch (final InterruptedException e) {
            throw new MessageTransportException("Interrupted while binding.", e);
        } catch (final RuntimeException e) {
            throw new MessageTransportException("Unexpected uncheked exception throw by netty.", e);
        } catch (final Exception e) {
            throw new MessageTransportException("Undeclared checked exception (yes, netty sucks) thrown by netty.", e);
        }

        // spin until we have it.
        return localAddr;
    }

    private static class Client<T> extends ByteToMessageDecoder {
        public final Serializer serializer;
        public final Listener<T> listener;

        Client(final Serializer serializer, final Supplier<Listener<T>> listener) {
            this.serializer = serializer;
            this.listener = listener.get();
        }

        static class ResetReaderIndex implements AutoCloseable {
            final ByteBuf buf;
            boolean clear = false;

            ResetReaderIndex(final ByteBuf buf) {
                this.buf = buf;
            }

            void clear() {
                clear = true;
            }

            @Override
            public void close() {
                if (!clear)
                    buf.resetReaderIndex();
            }
        }

        @Override
        protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
            if (in.readableBytes() < 2)
                return;

            try (ResetReaderIndex rr = new ResetReaderIndex(in.markReaderIndex());) {
                final short tmpsize = in.readShort();
                final int size;
                if (tmpsize == -1) {
                    if (in.readableBytes() < 4)
                        return;

                    size = in.readInt();
                } else {
                    size = tmpsize;
                }

                if (size < 1) { // we expect at least '1'
                    ReferenceCountUtil.release(in);
                    // assume we have a corrupt channel
                    throw new IOException("Read negative message size. Assuming a corrupt channel.");
                }

                if (in.readableBytes() < size)
                    return;

                final byte[] data = new byte[size];
                in.readBytes(data);
                rr.clear();
                try (MessageBufferInput mbi = new MessageBufferInput(data);) {
                    @SuppressWarnings("unchecked")
                    final T rm = (T) serializer.deserialize(mbi, RoutedMessage.class);
                    listener.onMessage(rm); // this should process the message asynchronously
                }
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws InterruptedException {
            // Close the connection when an exception is raised.
            LOGGER.error("Exception processing client connection. Closing...", cause);
            ctx.close().sync();
        }

    }
}
