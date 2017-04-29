package net.dempsy.transport.tcp.netty;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Router.RoutedMessage;
import net.dempsy.ServiceTracker;
import net.dempsy.TestWordCount;
import net.dempsy.serialization.Serializer;
import net.dempsy.serialization.jackson.JsonSerializer;
import net.dempsy.serialization.kryo.KryoSerializer;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.util.TestInfrastructure;

public class NettyTransportTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTransportTest.class);

    @Test
    public void testReceiverStart() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (ServiceTracker tr = new ServiceTracker();) {
            final NettyReceiver<RoutedMessage> r = tr.track(new NettyReceiver<RoutedMessage>(new JsonSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            r.start(null, tr.track(new DefaultThreadingModel(NettyTransportTest.class.getSimpleName() + ".testReceiverStart")));

            assertTrue(resolverCalled.get());
        }
    }

    @Test
    public void testMessage() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (ServiceTracker tr = new ServiceTracker();) {
            final NettyReceiver<RoutedMessage> r = tr.track(new NettyReceiver<RoutedMessage>(new KryoSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(NettyTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final NettySenderFactory sf = new NettySenderFactory();) {
                sf.start(new TestInfrastructure(null, null) {
                    @Override
                    public String getNodeId() {
                        return "test";
                    }
                });
                final NettySender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", "Hello"));

                assertTrue(resolverCalled.get());
                assertTrue(poll(o -> rm.get() != null));
                assertEquals("Hello", rm.get().message);
            }
        }
    }

    @Test
    public void testLargeMessage() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        final String huge = TestWordCount.readBible();
        try (ServiceTracker tr = new ServiceTracker();) {
            final NettyReceiver<RoutedMessage> r = tr.track(new NettyReceiver<RoutedMessage>(new JsonSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(NettyTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final NettySenderFactory sf = new NettySenderFactory();) {
                sf.start(new TestInfrastructure(null, null));
                final NettySender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", huge));

                assertTrue(resolverCalled.get());
                assertTrue(poll(o -> rm.get() != null));
                assertEquals(huge, rm.get().message);
            }
        }
    }

    private void runMultiMessage(final int numThreads, final int numMessagePerThread, final String message, final Serializer serializer)
            throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (final ServiceTracker tr = new ServiceTracker();) {
            final NettyReceiver<RoutedMessage> r = tr.track(new NettyReceiver<RoutedMessage>(serializer))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicLong msgCount = new AtomicLong();
            r.start((Listener<RoutedMessage>) msg -> {
                msgCount.incrementAndGet();
                return true;
            }, tr.track(new DefaultThreadingModel(NettyTransportTest.class.getSimpleName() + ".testReceiverStart")));

            final AtomicBoolean letMeGo = new AtomicBoolean(false);
            final List<Thread> threads = IntStream.range(0, numThreads).mapToObj(threadNum -> new Thread(() -> {
                try (final NettySenderFactory sf = new NettySenderFactory();) {
                    sf.start(new TestInfrastructure(null, null) {

                        @Override
                        public Map<String, String> getConfiguration() {
                            final Map<String, String> ret = new HashMap<>();
                            ret.put(NettySenderFactory.class.getPackage().getName() + "." + NettySenderFactory.CONFIG_KEY_SENDER_THREADS, "2");
                            return ret;
                        }

                    });
                    final NettySender sender = sf.getSender(addr);
                    while (!letMeGo.get())
                        Thread.yield();
                    for (int i = 0; i < numMessagePerThread; i++)
                        sender.send(new RoutedMessage(new int[] { 0 }, "Hello", message));

                }
            }, "testMultiMessage-Sender-" + threadNum))
                    .map(th -> chain(th, t -> t.start()))
                    .collect(Collectors.toList());
            Thread.sleep(10);

            // here's we go.
            letMeGo.set(true);

            // all threads should eventually exit.
            assertTrue(poll(threads, o -> o.stream().filter(t -> t.isAlive()).count() == 0));

            // the total number of messages sent should be this count.
            assertTrue(poll(new Long((long) numThreads * (long) numMessagePerThread), v -> v.longValue() == msgCount.get()));
        }
    }

    @Test
    public void testMultiMessage() throws Exception {
        runMultiMessage(10, 10000, "Hello", new KryoSerializer());
    }

    @Test
    public void testMultiHugeMessage() throws Exception {
        runMultiMessage(5, 100, TestWordCount.readBible(), new JsonSerializer());
    }
}
