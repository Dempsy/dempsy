package net.dempsy.transport.tcp.nio;

import static net.dempsy.transport.tcp.nio.internal.NioUtils.dontInterrupt;
import static net.dempsy.util.Functional.chain;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.util.StupidHashMap;

public class NioSenderFactory implements SenderFactory {
    public final static Logger LOGGER = LoggerFactory.getLogger(NioSenderFactory.class);

    public static final String CONFIG_KEY_SENDER_THREADS = "send_threads";
    public static final String DEFAULT_SENDER_THREADS = "2";

    // public static final String CONFIG_KEY_SENDER_BLOCKING = "send_blocking";
    // public static final String DEFAULT_SENDER_BLOCKING = "true";

    public static final String CONFIG_KEY_SENDER_MAX_QUEUED = "send_max_queued";
    public static final String DEFAULT_SENDER_MAX_QUEUED = "1000";

    public static final String CONFIG_KEY_SENDER_TCP_MTU = "tcp_mtu";
    public static final String DEFAULT_SENDER_TCP_MTU = "1400";

    public static final String CONFIG_KEY_SENDER_STOP_TIMEOUT_MILLIS = "sender_stop_timeout_millis";
    public static final String DEFAULT_SENDER_STOP_TIMEOUT_MILLIS = "3000";

    private final ConcurrentHashMap<TcpAddress, NioSender> senders = new ConcurrentHashMap<>();

    final StupidHashMap<NioSender, NioSender> working = new StupidHashMap<>();

    // =======================================
    // Read from NioSender
    final Manager<Serializer> serializerManager = new Manager<Serializer>(Serializer.class);
    final AtomicBoolean isRunning = new AtomicBoolean(true);
    NodeStatsCollector statsCollector;
    String nodeId;
    int maxNumberOfQueuedOutgoing;
    // boolean blocking;
    int mtu = Integer.parseInt(DEFAULT_SENDER_TCP_MTU);
    int stopTimeout = Integer.parseInt(DEFAULT_SENDER_STOP_TIMEOUT_MILLIS);
    // =======================================

    private Sending[] sendings;
    private Thread[] sendingsThreads;
    private final AtomicBoolean sendingsRunning = new AtomicBoolean(true);

    @Override
    public void close() {
        LOGGER.trace(nodeId + " stopping " + NioSenderFactory.class.getSimpleName());
        final List<NioSender> snapshot;
        synchronized (this) {
            isRunning.set(false);
            snapshot = new ArrayList<>(senders.values());
        }
        snapshot.forEach(s -> s.stop());

        // we SHOULD be all done.
        final boolean recurse;
        synchronized (this) {
            recurse = senders.size() > 0;
        }
        if (recurse)
            close();

        // all senders closed, we should stop the threads.
        sendingsRunning.set(false);
        final List<Thread> threads = new ArrayList<>(Arrays.asList(sendingsThreads));
        boolean done = false;
        final long startWaitTime = System.currentTimeMillis();
        while (threads.size() > 0 && !done) {
            done = true;
            for (final Iterator<Thread> iter = threads.iterator(); iter.hasNext();) {
                final Thread cur = iter.next();
                if (!cur.isAlive())
                    iter.remove();
                else
                    done = false;
            }
            if ((System.currentTimeMillis() - startWaitTime) > stopTimeout) {
                LOGGER.warn("Stopping without having been able to stop all sending threads.");
                done = true;
            }
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public NioSender getSender(final NodeAddress destination) throws MessageTransportException {
        final TcpAddress tcpaddr = (TcpAddress) destination;
        final NioSender ret;
        if (isRunning.get()) {
            ret = senders.computeIfAbsent(tcpaddr, a -> new NioSender(a, this));
        } else
            throw new MessageTransportException(nodeId + " sender had getSender called while stopped.");

        try {
            ret.connect();
        } catch (final IOException e) {
            throw new MessageTransportException(nodeId + " sender failed to connect to " + destination.getGuid(), e);
        }
        return ret;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.statsCollector = infra.getNodeStatsCollector();
        this.nodeId = infra.getNodeId();

        final int numSenderThreads = Integer
                .parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_THREADS, DEFAULT_SENDER_THREADS));

        mtu = Integer
                .parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_TCP_MTU, DEFAULT_SENDER_TCP_MTU));

        maxNumberOfQueuedOutgoing = Integer.parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_MAX_QUEUED, DEFAULT_SENDER_MAX_QUEUED));

        // blocking = Boolean.parseBoolean(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_BLOCKING, DEFAULT_SENDER_BLOCKING));

        stopTimeout = Integer
                .parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_STOP_TIMEOUT_MILLIS, DEFAULT_SENDER_STOP_TIMEOUT_MILLIS));

        sendings = new Sending[numSenderThreads];
        sendingsThreads = new Thread[numSenderThreads];

        // now start the sending threads.
        for (int i = 0; i < sendings.length; i++)
            chain(sendingsThreads[i] = new Thread(sendings[i] = new Sending(sendingsRunning, nodeId, working, statsCollector),
                    "nio-sender-" + i + "-" + nodeId), t -> t.start());

    }

    void imDone(final TcpAddress tcp) {
        senders.remove(tcp);
    }

    public static class Sending implements Runnable {
        final AtomicBoolean isRunning;
        final Selector selector;
        final String nodeId;
        final StupidHashMap<NioSender, NioSender> idleSenders;
        final NodeStatsCollector statsCollector;

        Sending(final AtomicBoolean isRunning, final String nodeId, final StupidHashMap<NioSender, NioSender> working,
                final NodeStatsCollector statsCollector)
                throws MessageTransportException {
            this.isRunning = isRunning;
            this.nodeId = nodeId;
            this.idleSenders = working;
            this.statsCollector = statsCollector;
            try {
                this.selector = Selector.open();
            } catch (final IOException e) {
                throw new MessageTransportException(e);
            }
        }

        @Override
        public void run() {
            int numNothing = 0;
            while (isRunning.get()) {
                try {
                    // blocking causes attempts to register to block creating a potential deadlock
                    final int numSelected = selector.selectNow();

                    // are there any sockets ready to write?
                    if (numSelected == 0) {
                        // =====================================================================
                        // nothing ready ... might as well spend some time serializing messages
                        final Set<SelectionKey> keys = selector.keys();
                        if (keys != null && keys.size() > 0) {
                            numNothing = 0; // reset the yield count since we have something to do
                            final SenderHolder thisOneCanSerialize = keys.stream()
                                    .map(k -> (SenderHolder) k.attachment())
                                    .filter(s -> !s.readyToWrite(true)) // if we're ready to write then we don't need to do more.
                                    .filter(s -> s.readyToSerialize())
                                    .findFirst()
                                    .orElse(null);
                            if (thisOneCanSerialize != null)
                                thisOneCanSerialize.trySerialize();
                        }
                        // =====================================================================
                        else { // nothing to serialize, do we have any new senders that need handling?
                            if (!checkForNewSenders()) { // if we didn't do anything then sleep/yield based on how long we've been bord.
                                numNothing++;
                                if (numNothing > 1000) {
                                    dontInterrupt(() -> Thread.sleep(1), ie -> {
                                        if (isRunning.get())
                                            LOGGER.error(nodeId + " sender interrupted", ie);
                                    });
                                } else
                                    Thread.yield();
                            } else // otherwise we DID do something
                                numNothing = 0;
                        }
                        continue;
                    } else
                        numNothing = 0; // reset the yield count since we have something to do

                    final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        final SelectionKey key = keys.next();

                        keys.remove();

                        if (!key.isValid())
                            continue;

                        if (key.isWritable()) {
                            final SenderHolder sh = (SenderHolder) key.attachment();
                            if (sh.writeSomethingReturnDone(key, statsCollector)) {
                                idleSenders.putIfAbsent(sh.sender, sh.sender);
                                key.cancel();
                            }
                        }
                    }
                } catch (final IOException ioe) {
                    LOGGER.error(nodeId + " sender failed", ioe);
                } finally {
                    // LOGGER.trace("looping sending thread:" + numNothing);
                }
            }
        }

        private boolean checkForNewSenders() throws IOException {
            boolean didSomething = false;
            final Set<NioSender> curSenders = idleSenders.keySet();
            final Set<NioSender> newSenders = new HashSet<>();

            try { // if we fail here we need to put the senders back or we'll loose them forever.

                // move any NioSenders with data from working and onto newSenders
                curSenders.stream()
                        .filter(s -> s.messages.peek() != null)
                        .forEach(s -> {
                            final NioSender cur = idleSenders.remove(s);
                            // removing them means putting them on the newSenders set so we can track them
                            if (cur != null)
                                newSenders.add(cur);
                        });

                // newSenders are now mine since they've been removed from working.

                // go through each new sender ...
                for (final Iterator<NioSender> iter = newSenders.iterator(); iter.hasNext();) {
                    final NioSender cur = iter.next();

                    // ... if the new sender has messages ...
                    if (cur.messages.peek() != null) {
                        // ... register the channel for writing and attach the SenderHolder
                        new SenderHolder(cur).register(selector);
                        iter.remove();
                        didSomething = true; // we did something.
                    }
                }
            } finally {
                // any still on toWork need to be returned to working
                newSenders.forEach(s -> idleSenders.putIfAbsent(s, s));
            }

            return didSomething;
        }

    }
}
