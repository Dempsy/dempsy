package net.dempsy;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.uncheck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Node;
import net.dempsy.container.Container;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.container.altnonlockingbulk.NonLockingAltBulkContainer;
import net.dempsy.container.locking.LockingContainer;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.Evictable;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.utils.test.ConditionPoll;

@RunWith(Parameterized.class)
public class TestAlmostSimple {

    final String containerTypeId;

    public TestAlmostSimple(final String containerTypeId) {
        this.containerTypeId = containerTypeId;

        // reset the value
        MpEven.allowEviction.set(false);
    }

    @Parameters(name = "container type={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {NonLockingAltContainer.class.getPackageName()},
            {NonLockingAltBulkContainer.class.getPackageName()},
            {LockingContainer.class.getPackageName()},
            // non-locking container is broken
            // {NonLockingContainer.class.getPackageName()},
        });
    }

    public static class Dummy implements Receiver {
        NodeAddress dummy = new NodeAddress() {
            private static final long serialVersionUID = 1L;
        };

        @Override
        public void close() {}

        @Override
        public NodeAddress getAddress(final Infrastructure infra) {
            return dummy;
        }

        @Override
        public void start(final Listener<?> listener, final Infrastructure threadingModel) throws MessageTransportException {}
    }

    @MessageType
    public static class Message implements Serializable {

        private static final long serialVersionUID = 1L;

        public int message;

        public Message(final int message) {
            this.message = message;
        }

        @MessageKey("even")
        public String even() {
            return ((message & 0x01) == 0x01) ? "no" : "yes";
        }

        @MessageKey("odd")
        public String odd() {
            return ((message & 0x01) == 0x01) ? "yes" : "no";
        }
    }

    @Mp
    public static class MpEven implements Cloneable {
        public List<MpEven> allMps = new ArrayList<>();
        public List<Message> received = new ArrayList<>();
        public boolean evictCalled = false;
        public String key = null;
        public static AtomicBoolean allowEviction = new AtomicBoolean(false);

        @Activation
        public void activate(final String evenp) {
            key = evenp;
        }

        @MessageHandler("even")
        public void handle(final Message message) {
            received.add(message);
        }

        @Evictable
        public boolean evictMe() {
            evictCalled = true;
            final boolean evictMe = allowEviction.get();
            System.out.println(key + " should evict:" + evictMe);
            return evictMe;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return chain(new MpEven(), m -> allMps.add(m));
        }
    }

    @Mp
    public static class MpOdd implements Cloneable {
        public List<MpOdd> allMps = new ArrayList<>();
        public List<Message> received = new ArrayList<>();
        public String key = null;

        @Activation
        public void activate(final String evenp) {
            key = evenp;
        }

        @MessageHandler("odd")
        public void handle(final Message message) {
            received.add(message);
        }

        @Override
        public MpOdd clone() throws CloneNotSupportedException {
            return chain(new MpOdd(), m -> allMps.add(m));
        }
    }

    @Test
    public void testSimple() throws Exception {
        final MpEven mpEven = new MpEven();
        final MpOdd mpOdd = new MpOdd();

        final AtomicLong count = new AtomicLong(0);

        Adaptor adaptor = null;
        try(final DefaultThreadingModel tm = new DefaultThreadingModel("TB", -1, 1);
            final NodeManager nm = new NodeManager();) {
            final Node n = new Node.Builder("test-app")
                .defaultRoutingStrategyId("net.dempsy.router.simple")
                .containerTypeId(containerTypeId)
                .receiver(new Dummy())
                .cluster("start")
                .adaptor(adaptor = new Adaptor() {
                    private Dispatcher disp;
                    boolean done = false;
                    int cur = 0;
                    AtomicBoolean exitedLoop = new AtomicBoolean(false);

                    @Override
                    public void stop() {
                        done = true;
                        while(!exitedLoop.get())
                            Thread.yield();
                    }

                    @Override
                    public void start() {
                        try {
                            while(!done) {
                                uncheck(() -> disp.dispatchAnnotated(new Message(cur++)));
                                count.incrementAndGet();
                            }
                        } finally {
                            exitedLoop.set(true);
                        }
                    }

                    @Override
                    public void setDispatcher(final Dispatcher dispatcher) {
                        this.disp = dispatcher;
                    }
                })
                .cluster("mpEven")
                .mp(new MessageProcessor<>(mpEven))
                .evictionFrequency(1, TimeUnit.SECONDS)
                .cluster("mpOdd")
                .mp(new MessageProcessor<>(mpOdd))
                .build();

            nm.node(n)
                .collaborator(new LocalClusterSessionFactory().createSession())
                .threadingModel(tm.start());

            nm.start();

            assertTrue(ConditionPoll.poll(c -> count.get() > 100000));

            // make sure an eviction happened before closing
            // at this point all of the mps should have been checked for eviction
            assertTrue(ConditionPoll.poll(o -> 2 == mpEven.allMps.size()));
            mpEven.allMps.forEach(m -> uncheck(() -> assertTrue(ConditionPoll.poll(o -> m.evictCalled))));

            // now enable them to actually be removed from the container.
            // we need to stop the adaptor or we'll keep recreating them
            adaptor.stop();
            Thread.sleep(2000); // need to wait for all of the adaptor messages to play through before evicting
            MpEven.allowEviction.set(true);
            final List<Container> containers = nm.getContainers().stream()
                .filter(c -> "mpEven".equals(c.getClusterId().clusterName))
                .collect(Collectors.toList());
            assertEquals(1, containers.size());
            final Container container = containers.get(0);
            assertTrue(ConditionPoll.poll(o -> 0 == container.getProcessorCount()));
        }

        Thread.sleep(2000);

        // make sure the messages were distributed correctly.
        assertEquals(2, mpEven.allMps.size());
        assertEquals(2, mpOdd.allMps.size());

        final MpEven mpEvenYes = mpEven.allMps.stream().filter(m -> "yes".equals(m.key)).findAny().get();
        // all messages here should be even numbers
        assertTrue(mpEvenYes.received.stream()
            .allMatch(m -> (m.message & 0x01) == 0));

        final MpEven mpEvenNo = mpEven.allMps.stream().filter(m -> "no".equals(m.key)).findAny().get();
        // all messages here should be odd numbers
        assertTrue(mpEvenNo.received.stream()
            .allMatch(m -> (m.message & 0x01) == 1));

        final MpOdd mpOddYes = mpOdd.allMps.stream().filter(m -> "yes".equals(m.key)).findAny().get();
        // all messages here should be odd numbers
        assertTrue(mpOddYes.received.stream()
            .allMatch(m -> (m.message & 0x01) == 1));

        final MpOdd mpOddNo = mpOdd.allMps.stream().filter(m -> "no".equals(m.key)).findAny().get();
        // all messages here should be even numbers
        assertTrue(mpOddNo.received.stream()
            .allMatch(m -> (m.message & 0x01) == 0));

    }

}
