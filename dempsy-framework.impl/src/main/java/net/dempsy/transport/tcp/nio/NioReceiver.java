package net.dempsy.transport.tcp.nio;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.serialization.Serializer;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.transport.tcp.AbstractTcpReceiver;
import net.dempsy.transport.tcp.TcpUtils;
import net.dempsy.transport.tcp.nio.internal.NioUtils;
import net.dempsy.transport.tcp.nio.internal.NioUtils.ReturnableBufferOutput;
import net.dempsy.util.io.MessageBufferInput;

public class NioReceiver<T> extends AbstractTcpReceiver<NioAddress, NioReceiver<T>> {
    private static Logger LOGGER = LoggerFactory.getLogger(NioReceiver.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private NioAddress internal = null;
    private NioAddress address = null;
    private Binding binding = null;
    private Acceptor acceptor = null;

    @SuppressWarnings("unchecked")
    private Reader<T>[] readers = new Reader[2];

    public NioReceiver(final Serializer serializer, final int port) {
        super(serializer, port);
    }

    public NioReceiver(final Serializer serializer) {
        this(serializer, -1);
    }

    @Override
    public void close() {
        isRunning.set(false);
        if (acceptor != null)
            acceptor.close();

        for (int i = 0; i < readers.length; i++) {
            final Reader<T> r = readers[i];
            if (r != null)
                r.close();
        }
    }

    @Override
    public synchronized NioAddress getAddress() {
        if (internal == null) {
            try {
                final InetAddress addr = useLocalHost ? Inet4Address.getLocalHost()
                        : (addrSupplier == null ? TcpUtils.getFirstNonLocalhostInetAddress() : addrSupplier.get());
                binding = new Binding(addr, internalPort);
                final InetSocketAddress inetSocketAddress = binding.bound;
                internalPort = inetSocketAddress.getPort();
                internal = new NioAddress(addr, internalPort, serId, binding.recvBufferSize);
                address = resolver.getExternalAddresses(internal);
            } catch (final IOException e) {
                throw new DempsyException(e);
            }
        }
        return address;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void start(final Listener<?> listener, final Infrastructure infra) throws MessageTransportException {
        if (!isRunning.get())
            throw new IllegalStateException("Cannot restart an " + NioReceiver.class.getSimpleName());

        if (binding == null)
            getAddress(); // sets binding via side affect.

        // before starting the acceptor, make sure we have Readers created.
        try {
            for (int i = 0; i < readers.length; i++)
                readers[i] = new Reader<T>(isRunning, address.getGuid(), (Listener<T>) listener, serializer, maxMessageSize);
        } catch (final IOException ioe) {
            LOGGER.error(address.getGuid() + " failed to start up readers", ioe);
            throw new MessageTransportException(address.getGuid() + " failed to start up readers", ioe);
        }

        final ThreadingModel threadingModel = infra.getThreadingModel();
        // now start the readers.
        for (int i = 0; i < readers.length; i++)
            threadingModel.runDaemon(readers[i], "nio-reader-" + i + "-" + address);

        // start the acceptor
        threadingModel.runDaemon(acceptor = new Acceptor(binding, isRunning, readers, address.getGuid()), "nio-acceptor-" + address);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioReceiver<T> setNumHandlers(final int numHandlers) {
        readers = new Reader[numHandlers];
        return this;
    }

    // =============================================================================
    // These classes manages accepting external connections.
    // =============================================================================
    public static class Binding {
        public final Selector selector;
        public final ServerSocketChannel serverChannel;
        public final InetSocketAddress bound;
        public final int recvBufferSize;

        public Binding(final InetAddress addr, final int port) throws IOException {
            final int lport = port < 0 ? 0 : port;
            selector = Selector.open();

            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);

            final InetSocketAddress tobind = new InetSocketAddress(addr, lport);
            final ServerSocket sock = serverChannel.socket();
            sock.bind(tobind);
            bound = (InetSocketAddress) sock.getLocalSocketAddress();
            recvBufferSize = sock.getReceiveBufferSize();
        }
    }

    private static class Acceptor implements Runnable {
        final Binding binding;
        final AtomicBoolean isRunning;
        final Reader<?>[] readers;
        final long numReaders;
        final AtomicLong messageNum = new AtomicLong(0);
        final AtomicBoolean done = new AtomicBoolean(false);
        final String thisNode;

        private Acceptor(final Binding binding, final AtomicBoolean isRunning, final Reader<?>[] readers, final String thisNode) {
            this.binding = binding;
            this.isRunning = isRunning;
            this.readers = readers;
            this.numReaders = readers.length;
            this.thisNode = thisNode;
        }

        @Override
        public void run() {
            final Selector selector = binding.selector;
            final ServerSocketChannel serverChannel = binding.serverChannel;

            try {
                while (isRunning.get()) {
                    try {
                        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                        while (isRunning.get()) {
                            final int numSelected = selector.select();

                            if (numSelected == 0)
                                continue;

                            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                            while (keys.hasNext()) {
                                final SelectionKey key = keys.next();

                                keys.remove();

                                if (!key.isValid())
                                    continue;

                                if (key.isAcceptable()) {
                                    accept(key);
                                }
                            }
                        }
                    } catch (final IOException ioe) {
                        LOGGER.error("Failed during accept loop.", ioe);
                    }
                }
            } finally {
                try {
                    serverChannel.close();
                } catch (final IOException e) {
                    LOGGER.error(thisNode + " had an error trying to close the accept socket channel.", e);
                }
                done.set(true);
            }
        }

        private void accept(final SelectionKey key) throws IOException {
            final Reader<?> reader = readers[(int) (messageNum.getAndIncrement() % numReaders)];

            final ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
            final SocketChannel channel = serverChannel.accept();

            LOGGER.trace(thisNode + " is accepting a connection from " + channel.getRemoteAddress());

            reader.newClient(channel);
        }

        // assumes isRunning is already set to false
        private void close() {
            while (!done.get()) {
                binding.selector.wakeup();
                Thread.yield();
            }
        }
    }
    // =============================================================================

    // =============================================================================
    // A Client instance is attached to each socket in the selector's register
    // =============================================================================
    private static class Client<T> {
        ReturnableBufferOutput partialRead = null;
        private final String thisNode;
        private final Listener<T> typedListener;
        private final Serializer serializer;
        private final int maxMessageSize;

        private Client(final String thisNode, final Listener<T> listener, final Serializer serializer, final int maxMessageSize) {
            this.thisNode = thisNode;
            this.typedListener = listener;
            this.serializer = serializer;
            this.maxMessageSize = maxMessageSize;
        }

        /**
         * Read the size
         * @return -1 if there aren't enough bytes read in to figure out the size. -2 if the
         * socket channel reached it's eof. Otherwise, the size actually read.
         */
        private final int readSize(final SocketChannel channel, final ByteBuffer bb) throws IOException {
            final int size;

            // if (bb.position() < 4) {
            // bb.limit(4);
            // if (channel.read(bb) == -1)
            // return -2;
            // }
            //
            // if (bb.position() >= 4)
            // size = bb.getInt(0);
            // else
            // size = -1;

            if (bb.position() < 2) {
                // read a Short
                bb.limit(2);
                if (channel.read(bb) == -1)
                    return -2;
            }

            if (bb.position() >= 2) { // we read the full short in
                final short ssize = bb.getShort(0); // read the short.

                if (ssize == -1) { // we need to read the int ... indication that an int size is there.
                    if (bb.position() < 6) {
                        bb.limit(6); // set the limit to read the int.
                        if (channel.read(bb) == -1) // read 4 more bytes.
                            return -2;
                    }

                    if (bb.position() >= 6) // we have an int based size
                        size = bb.getInt(2); // read an int starting after the short.
                    else
                        // we need an int based size but don't have it all yet.
                        size = -1; // we're going to need to try again.

                } else { // the ssize contains the full size.
                    size = ssize;
                }
            } else {
                // we already tried to read the short but didn't get enought bytes.
                size = -1; // try again.
            }

            return size;
        }

        private void closeup(final SocketChannel channel, final SelectionKey key) {
            final Socket socket = channel.socket();
            final SocketAddress remoteAddr = socket.getRemoteSocketAddress();
            LOGGER.debug(thisNode + " had a connection closed by client: " + remoteAddr);
            try {
                channel.close();
            } catch (final IOException ioe) {
                LOGGER.error(thisNode + " failed to close the receiver channel receiving data from " + remoteAddr + ". Ingoring", ioe);
            }
            key.cancel();
        }

        public void read(final SelectionKey key) throws IOException {
            final SocketChannel channel = (SocketChannel) key.channel();
            final ReturnableBufferOutput buf;
            if (partialRead == null) {
                buf = NioUtils.get();
                buf.getBb().limit(2); // set it to read the short for size initially
                partialRead = buf; // set the partialRead. We'll unset this when we pass it on
            } else
                buf = partialRead;
            ByteBuffer bb = buf.getBb();

            if (bb.limit() <= 6) { // we haven't read the size yet.
                final int size = readSize(channel, bb);
                if (size == -2) { // indication we hit an eof
                    closeup(channel, key);
                    return; // we're done
                }
                if (size == -1) { // we didn't read the size yet so just go back.
                    return;
                }
                // if the results are less than zero or WAY to big, we need to assume a corrupt channel.
                if (size <= 0 || size > maxMessageSize) {
                    // assume the channel is corrupted and close us out.
                    LOGGER.warn(thisNode + " received what appears to be a corrupt message because it's size is " + size);
                    closeup(channel, key);
                    return;
                }

                final int limit = bb.limit();
                if (bb.capacity() < limit + size) {
                    // we need to grow the underlying buffer.
                    buf.grow(limit + size);
                    bb = buf.getBb();
                }

                buf.messageStart = bb.position();
                bb.limit(limit + size); // set the limit to read the entire message.
            }

            if (bb.position() < bb.limit()) {
                // continue reading
                if (channel.read(bb) == -1) {
                    closeup(channel, key);
                    return;
                }
            }

            if (bb.position() < bb.limit())
                return; // we need to wait for more data.

            // otherwise we have a message ready to go.
            final ReturnableBufferOutput toGo = partialRead;
            partialRead = null;
            typedListener.onMessage(() -> {
                try (final ReturnableBufferOutput mbo = toGo;
                        final MessageBufferInput mbi = new MessageBufferInput(mbo.getBuffer(), mbo.messageStart, mbo.getBb().position());) {
                    @SuppressWarnings("unchecked")
                    final T rm = (T) serializer.deserialize(mbi, RoutedMessage.class);
                    return rm;
                } catch (final IOException ioe) {
                    LOGGER.error(thisNode + " failed on deserialization", ioe);
                    throw new DempsyException(ioe);
                }
            });
        }
    }

    public static class Reader<T> implements Runnable {

        private final AtomicReference<SocketChannel> landing = new AtomicReference<SocketChannel>(null);
        private final Selector selector;
        private final AtomicBoolean isRunning;
        private final String thisNode;
        private final Listener<T> typedListener;
        private final Serializer serializer;
        private final int maxMessageSize;
        private final AtomicBoolean done = new AtomicBoolean(false);

        public Reader(final AtomicBoolean isRunning, final String thisNode, final Listener<T> typedListener, final Serializer serializer,
                final int maxMessageSize)
                throws IOException {
            selector = Selector.open();
            this.isRunning = isRunning;
            this.thisNode = thisNode;
            this.typedListener = typedListener;
            this.serializer = serializer;
            this.maxMessageSize = maxMessageSize;
        }

        @Override
        public void run() {

            try {
                while (isRunning.get()) {
                    try {
                        final int numKeysSelected = selector.select();

                        if (numKeysSelected > 0) {
                            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                            while (keys.hasNext()) {
                                final SelectionKey key = keys.next();

                                keys.remove();

                                if (!key.isValid())
                                    continue;

                                if (key.isReadable()) {
                                    ((Client<?>) key.attachment()).read(key);
                                } else // this shouldn't be possible
                                    LOGGER.info(thisNode + " reciever got an unexpexted selection key " + key);
                            }
                        } else if (isRunning.get() && !done.get()) {

                            // if we processed no keys then maybe we have a new client passed over to us.
                            final SocketChannel newClient = landing.getAndSet(null); // mark it as retrieved.
                            if (newClient != null) {
                                // we have a new client
                                newClient.configureBlocking(false);
                                final Socket socket = newClient.socket();
                                final SocketAddress remote = socket.getRemoteSocketAddress();
                                LOGGER.debug(thisNode + " received connection from " + remote);
                                newClient.register(selector, SelectionKey.OP_READ,
                                        new Client<T>(thisNode, typedListener, serializer, maxMessageSize));
                            }
                        }
                    } catch (final IOException ioe) {
                        LOGGER.error("Failed during read loop.", ioe);
                    }
                }
            } finally {
                done.set(true);
            }
        }

        // assumes isRunning is already set to false
        private void close() {
            while (!done.get()) {
                selector.wakeup();
                Thread.yield();
            }
        }

        public synchronized void newClient(final SocketChannel newClient) {
            // attempt to set the landing as long as it's null
            while (landing.compareAndSet(null, newClient))
                Thread.yield();

            // wait until the Reader runnable takes it.
            while (landing.get() != null && isRunning.get() && !done.get()) {
                selector.wakeup();
                Thread.yield();
            }
        }
    }

}
