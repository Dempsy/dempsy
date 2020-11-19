
package net.dempsy.router.shardutils;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.config.ClusterId;
import net.dempsy.router.BaseRouterTestWithSession;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.transport.NodeAddress;

public class TestLeaderAndSubscriber extends BaseRouterTestWithSession {
    static final Logger LOGGER = LoggerFactory.getLogger(TestLeaderAndSubscriber.class);

    public TestLeaderAndSubscriber(final Supplier<ClusterInfoSessionFactory> factory, final String disruptorName,
        final Consumer<ClusterInfoSession> disruptor) {
        super(LOGGER, factory.get(), disruptor);
    }

    @Test
    public void testOneLeader() throws InterruptedException {
        final ClusterId cid = setTestName("testOneLeader");
        final Utils<ContainerAddress> utils = new Utils<ContainerAddress>(makeInfra(session, sched), cid.clusterName,
            new ContainerAddress(new DummyNodeAddress(), new int[] {0}));

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        try {
            final Leader<ContainerAddress> l = new Leader<ContainerAddress>(utils, 256, 1, infra, isRunning, ContainerAddress[]::new);
            l.process();
            assertTrue(poll(o -> l.imIt()));
        } finally {
            isRunning.set(false);
        }
    }

    static class ListenerHolder {
        public final ClusterInfoSession session;
        public final Leader<ContainerAddress> l;
        public final Thread t;

        public ListenerHolder(final ClusterInfoSession session, final Leader<ContainerAddress> l, final Thread t) {
            this.session = session;
            this.l = l;
            this.t = t;
        }
    }

    @Test
    public void testMultiLeader() throws Exception {
        final int NUM_THREADS = 10;

        final ClusterId cid = setTestName("testMultiLeader");
        final AtomicBoolean isRunning = new AtomicBoolean(true);
        final AtomicBoolean go = new AtomicBoolean(false);
        final List<ListenerHolder> allHolders = new ArrayList<>();
        try {
            final List<ListenerHolder> holders = new ArrayList<>();

            for(int i = 0; i < NUM_THREADS; i++) {
                final ClusterInfoSession session = sessFact.createSession();
                final Utils<ContainerAddress> utils = new Utils<>(makeInfra(session, sched), cid.clusterName,
                    new ContainerAddress(new DummyNodeAddress(), new int[] {0}));
                final Leader<ContainerAddress> l = new Leader<>(utils, 256, 1, infra, isRunning, ContainerAddress[]::new);

                final Thread t = new Thread(() -> {
                    // wait for it.
                    while(!go.get()) {
                        if(!isRunning.get())
                            return;
                        Thread.yield();
                    }
                    l.process();
                }, "testMultiLeader-" + i);
                holders.add(new ListenerHolder(session, l, t));
                t.start();
            }

            // all threads running.
            Thread.sleep(50);

            allHolders.addAll(holders); // we're going to modify holders

            // let 'em go.
            go.set(true);

            // once they're all ready, one should be 'it'
            assertTrue(poll(o -> holders.stream().map(h -> h.l).filter(l -> l.isReady()).count() == NUM_THREADS));
            assertEquals(1, holders.stream().map(h -> h.l).filter(l -> l.imIt()).count());

            // find the leader and kill it.
            final ListenerHolder h = holders.stream().filter(l -> l.l.imIt()).findFirst().get();
            disruptor.accept(h.session);

            holders.remove(h);
            assertTrue(poll(o -> holders.stream().filter(l -> l.l.imIt()).count() == 1L));
            Thread.sleep(50);
            assertEquals(1, holders.stream().filter(l -> l.l.imIt()).count());
            isRunning.set(false);
            assertTrue(poll(o -> holders.stream().filter(l -> l.t.isAlive()).count() == 0));
        } finally {
            allHolders.forEach(h -> h.session.close());
            isRunning.set(false);
        }
    }

    @Test
    public void testLeaderWithSubscriber() throws Exception {
        final ClusterId cid = setTestName("testLeaderWithSubscriber");
        final Utils<ContainerAddress> utils = new Utils<>(makeInfra(session, sched), cid.clusterName,
            new ContainerAddress(new DummyNodeAddress(), new int[] {0}));

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        try {
            final Subscriber<ContainerAddress> s = new Subscriber<>(utils, infra, isRunning, (l, m) -> {}, 256);
            s.process();
            final Leader<ContainerAddress> l = new Leader<>(utils, 256, 1, infra, isRunning, ContainerAddress[]::new);
            l.process();
            assertTrue(poll(o -> l.imIt()));

            // wait until it owns 1
            assertTrue(poll(o -> s.isReady()));

            // s should now own all shards.
            for(int i = 0; i < 256; i++)
                assertTrue(s.doIOwnShard(i));
        } finally {
            isRunning.set(false);
        }
    }

    @Test
    public void testLeaderWithMutipleSubscribers() throws Exception {
        final int NUM_SUBS = 10;
        final ClusterId cid = setTestName("testLeaderWithMutipleSubscribers");

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        final List<Thread> threads = new ArrayList<>();
        try {
            final List<Subscriber<ContainerAddress>> subs = new ArrayList<>();
            final AtomicBoolean go = new AtomicBoolean(false);

            for(int i = 0; i < NUM_SUBS; i++) {
                final Utils<ContainerAddress> utils = new Utils<>(makeInfra(session, sched), cid.clusterName,
                    new ContainerAddress(new DummyNodeAddress(), new int[] {0}));
                final Subscriber<ContainerAddress> s;
                subs.add(s = new Subscriber<>(utils, infra, isRunning, (l, m) -> {}, 256));
                final Thread t = new Thread(() -> {
                    // wait for it.
                    while(!go.get()) {
                        if(!isRunning.get())
                            return;
                        Thread.yield();
                    }
                    s.process();
                }, "testLeaderWithMutipleSubscribers-" + i);
                t.start();
                threads.add(t);
            }

            go.set(true);

            final Utils<ContainerAddress> utils = new Utils<>(makeInfra(session, sched), cid.clusterName, subs.get(3).getUtils().thisNodeAddress);
            final Leader<ContainerAddress> l = new Leader<>(utils, 256, 1, infra, isRunning, ContainerAddress[]::new);
            l.process();
            assertTrue(poll(o -> l.imIt()));

            // wait until ready
            assertTrue(poll(o -> subs.stream().filter(s -> s.isReady()).count() == NUM_SUBS));

            final int lowerNum = Math.floorDiv(256, NUM_SUBS);
            final int upperNum = (int)Math.ceil((double)256 / NUM_SUBS);

            // do we have balanced subs?
            assertTrue(poll(o -> subs.stream()
                .filter(s -> s.numShardsIOwn() >= lowerNum)
                .filter(s -> s.numShardsIOwn() <= upperNum)
                .count() == NUM_SUBS));

            isRunning.set(false);
            assertTrue(poll(o -> threads.stream().filter(t -> t.isAlive()).count() == 0));
        } finally {
            isRunning.set(false);
        }
    }

    public static class DummyNodeAddress implements NodeAddress {
        private static final long serialVersionUID = 1L;
        private static AtomicLong sequence = new AtomicLong(0);

        private final Long id;

        public DummyNodeAddress(final Long id) {
            this.id = id;
        }

        public DummyNodeAddress() {
            this(Long.valueOf(sequence.getAndIncrement()));
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            final DummyNodeAddress other = (DummyNodeAddress)obj;
            if(!id.equals(other.id))
                return false;
            return true;
        }

    }
}
