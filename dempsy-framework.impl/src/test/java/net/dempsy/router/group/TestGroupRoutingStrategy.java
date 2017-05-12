package net.dempsy.router.group;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.BaseRouterTestWithSession;
import net.dempsy.router.RoutingInboundManager;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.router.group.intern.GroupDetails;
import net.dempsy.router.shardutils.Utils;
import net.dempsy.router.shardutils.Utils.ShardAssignment;
import net.dempsy.transport.NodeAddress;
import net.dempsy.util.TestInfrastructure;

public class TestGroupRoutingStrategy extends BaseRouterTestWithSession {
    static final Logger LOGGER = LoggerFactory.getLogger(TestGroupRoutingStrategy.class);

    public TestGroupRoutingStrategy(final Supplier<ClusterInfoSessionFactory> factory, final String disruptorName,
            final Consumer<ClusterInfoSession> disruptor) {
        super(LOGGER, factory.get(), disruptor);
    }

    @Test
    public void testInboundSimpleHappyPathRegister() throws Exception {
        final int numShardsToExpect = Integer.parseInt(Utils.DEFAULT_TOTAL_SHARDS);
        final RoutingInboundManager manager = new RoutingInboundManager();
        try (final RoutingStrategy.Inbound ib = manager
                .getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":testInboundSimpleHappyPathRegister");) {
            final Utils<Object> msutils = new Utils<>(infra, "testInboundSimpleHappyPathRegister", new Object());

            assertNotNull(ib);
            assertTrue(ClusterGroupInbound.Proxy.class.isAssignableFrom(ib.getClass()));

            ib.setContainerDetails(new ClusterId("test", "test"), new ContainerAddress(new DummyNodeAddress("testInboundSimpleHappyPathRegister"), 0),
                    (l, m) -> {});
            ib.start(infra);

            assertTrue(waitForShards(session, msutils, numShardsToExpect));
        }
    }

    private static class MutableInt {
        public int val;
    }

    private static void checkForShardDistribution(final ClusterInfoSession session, final Utils<GroupDetails> utils, final int numShardsToExpect,
            final int numNodes) throws InterruptedException {
        final MutableInt iters = new MutableInt();
        assertTrue(poll(o -> {
            try {
                iters.val++;
                final boolean showLog = LOGGER.isTraceEnabled() && (iters.val % 100 == 0);
                final List<ShardAssignment<?>> sas = getShardAssignments(utils);
                final Set<Integer> shards = getCurrentShards(sas);
                if (shards.size() != numShardsToExpect) {
                    if (showLog)
                        LOGGER.trace("Not all shards available. Expecting {} but got {}", numShardsToExpect, shards.size());
                    return false;
                }
                final Map<NodeAddress, AtomicInteger> counts = new HashMap<>();
                for (final ShardAssignment<?> sa : sas) {
                    final GroupDetails cur = (GroupDetails) sa.addr;
                    AtomicInteger count = counts.get(cur.node);
                    if (count == null) {
                        count = new AtomicInteger(0);
                        counts.put(cur.node, count);
                    }
                    count.addAndGet(sa.shards.length);
                }

                if (counts.size() != numNodes) {
                    if (showLog)
                        LOGGER.trace("Not all nodes registered. {} out of {}", counts.size(), numNodes);
                    return false;
                }
                for (final Map.Entry<NodeAddress, AtomicInteger> entry : counts.entrySet()) {
                    if (Math.abs(entry.getValue().get() - (numShardsToExpect / numNodes)) > 1) {
                        if (showLog)
                            LOGGER.trace("Counts for {} is below what's expected. {} is not 1 away from " + (numShardsToExpect / numNodes) + ")",
                                    entry.getKey(), entry.getValue());
                        return false;
                    }
                }
                return true;
            } catch (final ClusterInfoException cie) {
                return false;
            }
        }));

    }

    @Test
    public void testInboundDoubleHappyPathRegister() throws Exception {
        final int numShardsToExpect = Integer.parseInt(Utils.DEFAULT_TOTAL_SHARDS);

        final String groupName = "testInboundDoubleHappyPathRegister";
        try (final RoutingStrategy.Inbound ib1 = new RoutingInboundManager()
                .getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName);
                final RoutingStrategy.Inbound ib2 = new RoutingInboundManager()
                        .getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName);) {
            final ClusterId clusterId = new ClusterId("test", "test");
            final NodeAddress node1Addr = new DummyNodeAddress("node1");
            final GroupDetails gd1 = new GroupDetails(groupName, node1Addr);
            final ContainerAddress node1Ca = new ContainerAddress(node1Addr, 0);
            final Utils<GroupDetails> utils = new Utils<>(infra, groupName, gd1);

            ib1.setContainerDetails(clusterId, node1Ca, (l, m) -> {});
            ib1.start(infra);

            final NodeAddress node2Addr = new DummyNodeAddress("node2");
            final ContainerAddress node2Ca = new ContainerAddress(node2Addr, 0);

            ib2.setContainerDetails(clusterId, node2Ca, (l, m) -> {});
            try (final ClusterInfoSession session2 = sessFact.createSession();) {
                ib2.start(new TestInfrastructure(session2, infra.getScheduler()));

                assertTrue(waitForShards(session, utils, numShardsToExpect));

                // if this worked right then numShardsToExpect/2 should be owned by each ... eventually.
                checkForShardDistribution(session, utils, numShardsToExpect, 2);

                // disrupt the session. This should cause a reshuffle but not fail
                disruptor.accept(session2);

                // everything should settle back
                checkForShardDistribution(session, utils, numShardsToExpect, 2);

                // now kill the second session.
                session2.close(); // this will disconnect the second Inbound and so the first should take over

                // see if we now have 1 session and it has all shards
                checkForShardDistribution(session, utils, numShardsToExpect, 1);
            }
        }
    }

    @Test
    public void testInboundResillience() throws Exception {
        final int numShardsToExpect = Integer.parseInt(Utils.DEFAULT_TOTAL_SHARDS);
        final String groupName = "testInboundResillience";
        final Manager<RoutingStrategy.Inbound> manager = new RoutingInboundManager();
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName);) {
            final ClusterId clusterId = super.setTestName("testInboundResillience");
            final NodeAddress na = new DummyNodeAddress("theOnlyNode");
            final ContainerAddress ca = new ContainerAddress(na, 0);
            final GroupDetails gd = new GroupDetails(groupName, na);
            final Infrastructure infra = makeInfra(session, sched);
            final Utils<GroupDetails> msutils = new Utils<>(infra, groupName, gd);

            ib.setContainerDetails(clusterId, ca, (l, m) -> {});
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            disruptor.accept(session);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);
        }
    }

    @Test
    public void testInboundWithOutbound() throws Exception {
        final int numShardsToExpect = Integer.parseInt(Utils.DEFAULT_TOTAL_SHARDS);
        final String groupName = "testInboundWithOutbound";
        final Manager<RoutingStrategy.Inbound> manager = new RoutingInboundManager();
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName);) {
            final ClusterId cid = setTestName("testInboundWithOutbound");
            final NodeAddress na = new DummyNodeAddress("here");
            final ContainerAddress oca = new ContainerAddress(na, 0);
            final Infrastructure infra = makeInfra(session, sched);
            final GroupDetails ogd = new GroupDetails(groupName, na);
            ogd.caByCluster.put(cid.clusterName, oca);
            final Utils<GroupDetails> msutils = new Utils<>(infra, groupName, ogd);

            ib.setContainerDetails(cid, oca, (l, m) -> {});
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            try (final ClusterInfoSession ses2 = sessFact.createSession();
                    final RoutingStrategyManager obman = chain(new RoutingStrategyManager(), o -> o.start(makeInfra(ses2, sched)));
                    final RoutingStrategy.Factory obf = obman
                            .getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName);) {

                obf.start(makeInfra(ses2, sched));

                assertTrue(poll(o -> obf.isReady()));

                final RoutingStrategy.Router ob = obf.getStrategy(cid);
                assertTrue(poll(o -> obf.isReady()));

                final KeyedMessageWithType km = new KeyedMessageWithType(new Object(), new Object(), "");
                assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));

                final ContainerAddress ca = ob.selectDestinationForMessage(km);
                assertNotNull(ca);

                assertEquals("here", ((DummyNodeAddress) ca.node).name);

                // now disrupt the session
                session.close();

                // the destination should clear until a new one runs
                // NO: destination will not necessarily clear.
                // poll(o -> ob.selectDestinationForMessage(km) == null);

                final NodeAddress nna = new DummyNodeAddress("here-again");
                final ContainerAddress nca = new ContainerAddress(nna, 0);
                ogd.caByCluster.put(cid.clusterName, oca);

                try (ClusterInfoSession ses3 = sessFact.createSession();
                        RoutingStrategy.Inbound ib2 = new RoutingInboundManager()
                                .getAssociatedInstance(ClusterGroupInbound.class.getPackage().getName() + ":" + groupName)) {
                    ib2.setContainerDetails(cid, nca, (l, m) -> {});
                    ib2.start(makeInfra(ses3, sched));

                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));
                }
            }
        }
    }

    private static class DummyNodeAddress implements NodeAddress {
        private static final long serialVersionUID = 1L;
        public final String name;

        @SuppressWarnings("unused")
        private DummyNodeAddress() {
            name = null;
        }

        public DummyNodeAddress(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            return name.equals(((DummyNodeAddress) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return "DummyNodeAddress[ " + name + " ]";
        }
    }

    @SuppressWarnings("unchecked")
    private static List<ShardAssignment<?>> getShardAssignments(final Utils<?> utils) throws ClusterInfoException {
        return (List<ShardAssignment<?>>) utils.persistentGetData(utils.shardsAssignedDir, null);
    }

    private static Set<Integer> getCurrentShards(final Utils<?> utils) throws ClusterInfoException {
        return getCurrentShards(getShardAssignments(utils));
    }

    private static Set<Integer> getCurrentShards(final List<ShardAssignment<?>> sas) {
        if (sas == null)
            return new HashSet<>();
        final Set<Integer> shards = sas.stream().map(sa -> Arrays.stream(sa.shards)
                .mapToObj(i -> Integer.valueOf(i)))
                .flatMap(i -> i)
                .collect(Collectors.toSet());
        return shards;
    }

    private static boolean waitForShards(final ClusterInfoSession session, final Utils<?> utils, final int shardCount)
            throws InterruptedException {
        return poll(o -> {
            try {
                return getCurrentShards(utils).size() == shardCount;
            } catch (final ClusterInfoException e) {
                return false;
            }
        });
    }
}
