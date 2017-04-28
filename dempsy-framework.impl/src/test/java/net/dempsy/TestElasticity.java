package net.dempsy;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.assertTrue;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.container.NodeMetricGetters;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.transport.NodeAddress;

public class TestElasticity extends DempsyBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestElasticity.class);

    private static final int profilerTestNumberCount = 100000;

    public static final String[][] actxPath = {
            { "elasticity/adaptor.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-rank.xml", },
    };

    public TestElasticity(final String routerId, final String containerId, final String sessCtx, final String tpCtx, final String serType) {
        super(LOGGER, routerId, containerId, sessCtx, tpCtx, serType);
    }

    // ========================================================================
    // Test classes we will be working with. The old word count example modified.
    // ========================================================================
    @MessageType
    public static class Number implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer number;
        private int rankIndex;

        public Number() {} // needed for kryo-serializer

        public Number(final Integer number, final int rankIndex) {
            this.number = number;
            this.rankIndex = rankIndex;
        }

        @MessageKey
        public Integer getNumber() {
            return number;
        }

        @Override
        public String toString() {
            return "" + number + "[" + rankIndex + "]";
        }
    }

    @MessageType
    public static class VerifyNumber extends Number implements Serializable {
        private static final long serialVersionUID = 1L;

        public VerifyNumber() {}

        public VerifyNumber(final Integer number, final int rankIndex) {
            super(number, rankIndex);
        }

        @Override
        public String toString() {
            return "verifying " + super.toString();
        }
    }

    @MessageType
    public static class NumberCount implements Serializable {
        private static final long serialVersionUID = 1L;
        public Integer number;
        public long count;
        public int rankIndex;

        public NumberCount(final Number number, final long count) {
            this.number = number.getNumber();
            this.count = count;
            this.rankIndex = number.rankIndex;
        }

        public NumberCount() {} // for kryo

        static final Integer one = new Integer(1);

        @MessageKey
        public Integer getKey() {
            return number;
        }

        @Override
        public String toString() {
            return "(" + count + " " + number + "s)[" + rankIndex + "]";
        }
    }

    public static class NumberProducer implements Adaptor {
        public Dispatcher dispatcher = null;

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    @Mp
    public static class NumberCounter implements Cloneable {
        // This will be shared among them all in a container due to the cloning semantics
        public AtomicLong messageCount = new AtomicLong(0);
        long counter = 0;
        String wordText;

        @Activation
        public void initMe(final String key) {
            this.wordText = key;
        }

        @MessageHandler
        public NumberCount handle(final Number word) {
            LOGGER.trace("NumberCount recevied {}", word);
            messageCount.incrementAndGet();
            return new NumberCount(word, counter++);
        }

        @Override
        public NumberCounter clone() throws CloneNotSupportedException {
            return (NumberCounter) super.clone();
        }
    }

    public static class Rank {
        public final Integer number;
        public final Long rank;

        public Rank(final Integer number, final long rank) {
            this.number = number;
            this.rank = rank;
        }

        @Override
        public String toString() {
            return "[ " + number + " count:" + rank + " ]";
        }
    }

    @Mp
    public static class NumberRank implements Cloneable {
        public final AtomicLong totalMessages = new AtomicLong(0);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final public AtomicReference<Map<Integer, Long>[]> countMap = new AtomicReference(new Map[1000]);

        {
            for (int i = 0; i < 1000; i++)
                countMap.get()[i] = new ConcurrentHashMap<Integer, Long>();
        }

        @MessageHandler
        public void handle(final NumberCount wordCount) {
            totalMessages.incrementAndGet();
            countMap.get()[wordCount.rankIndex].put(wordCount.number, wordCount.count);
        }

        @Override
        public NumberRank clone() throws CloneNotSupportedException {
            return (NumberRank) super.clone();
        }

        public List<Rank> getPairs(final int rankIndex) {
            final List<Rank> ret = new ArrayList<>(countMap.get()[rankIndex].size() + 10);
            for (final Map.Entry<Integer, Long> cur : countMap.get()[rankIndex].entrySet())
                ret.add(new Rank(cur.getKey(), cur.getValue()));
            Collections.sort(ret, (o1, o2) -> o2.rank.compareTo(o1.rank));
            return ret;
        }
    }

    // ========================================================================

    private static class MutableInt {
        public int val;
    }

    protected void waitForEvenShardDistribution(final ClusterInfoSession session, final String cluster, final int numNodes,
            final List<NodeManagerWithContext> nodes) throws InterruptedException {
        waitForEvenShardDistribution(session, cluster, NUM_MICROSHARDS, numNodes, nodes);
    }

    protected void waitForEvenShardDistribution(final ClusterInfoSession session, final String cluster, final int numShardsToExpect,
            final int numNodes, final List<NodeManagerWithContext> nodes) throws InterruptedException {
        final MutableInt iters = new MutableInt();
        iters.val = 0;
        // now wait until we can see it from every other node.
        assertTrue(poll(o -> {
            iters.val++;
            final boolean showLog = LOGGER.isTraceEnabled() && (iters.val % 100 == 0);
            final List<Set<NodeAddress>> reachable = nodes.stream()
                    // pick off the Router
                    .map(n -> n.manager.getRouter())
                    // gather the collection of reachable ContainerAddresses from each Router to the given cluster.
                    .map(r -> {
                        if (showLog)
                            LOGGER.trace("From {}", r.thisNode());
                        return r.allReachable(cluster);
                    })
                    // extract a set of NodeAddresses from each of the ContainerAddress collections
                    .map(c -> {
                        final Set<NodeAddress> ret = c.stream().map(ca -> ca.node).collect(Collectors.toSet());
                        if (showLog)
                            LOGGER.trace(" ... can see {}", ret);
                        return ret;
                    })
                    // Gather up the sets of NodeAddresses into a list, one for each Router.
                    .collect(Collectors.toList());

            // go through each reachable set and make sure they each contain all
            // of the appropriate destinations.
            for (final Set<NodeAddress> fromOne : reachable) {
                if (fromOne.size() != numNodes)
                    return false;
            }
            return true;
        }));
    }

    @Test
    public void testForProfiler() throws Throwable {
        try {
            // set up the test.
            final Number[] numbers = new Number[profilerTestNumberCount];
            final Random random = new Random();
            for (int i = 0; i < numbers.length; i++)
                numbers[i] = new Number(random.nextInt(1000), 0);

            final KeyExtractor ke = new KeyExtractor();

            runCombos("testForProfiler", (r, c, s, t, ser) -> {
                final boolean ret = elasticRouterIds.contains(r);
                if (ret) {
                    LOGGER.info("=====================================================================================");
                    LOGGER.info("======== Running testForProfiler with " + r + ", " + c + ", " + s + ", " + t);
                }
                return ret;
            }, actxPath, new String[][][] {
                    null,
                    { { "min_nodes", "3" } },
                    { { "min_nodes", "3" } },
                    { { "min_nodes", "3" } },
                    null,
            }, ns -> {
                final List<NodeManagerWithContext> nodes = ns.nodes;

                // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
                final NumberRank rank = nodes.get(4).ctx.getBean(NumberRank.class);

                try (final ClusterInfoSession session = ns.sessionFactory.createSession();) {
                    waitForEvenShardDistribution(session, "test-cluster1", 3, nodes);

                    // get the Adaptor Router's statCollector
                    final List<NodeMetricGetters> scs = nodes.stream()
                            .map(nwm -> nwm.manager)
                            .map(nm -> nm.getNodeStatsCollector())
                            .map(sc -> (NodeMetricGetters) sc)
                            .collect(Collectors.toList());

                    // grab the adaptor from the 0'th cluster + the 0'th (only) node.
                    final NumberProducer adaptor = nodes.get(0).ctx.getBean(NumberProducer.class);

                    // grab access to the Dispatcher from the Adaptor
                    final Dispatcher dispatcher = adaptor.dispatcher;

                    final long startTime = System.currentTimeMillis();

                    for (int i = 0; i < numbers.length; i++)
                        dispatcher.dispatch(ke.extract(numbers[i]));

                    LOGGER.info("====> Checking exact count.");

                    // keep going as long as they are trickling in.
                    assertTrue(
                            () -> {
                                IntStream.range(0, scs.size()).forEach(i -> {
                                    System.out.println("======> " + i);
                                    final NodeMetricGetters mg = scs.get(i);
                                    if (mg != null) {
                                        System.out.println("discarded: " + mg.getDiscardedMessageCount());
                                        System.out.println("not sent: " + mg.getMessagesNotSentCount());
                                    }
                                });
                                return "expected: " + profilerTestNumberCount + " but was: "
                                        + (rank.totalMessages.get() + scs.get(0).getMessagesNotSentCount())
                                        + ", (delivered: " + rank.totalMessages.get() + ", not sent: " + scs.get(0).getMessagesNotSentCount() + ")";
                            },
                            poll(o -> profilerTestNumberCount == (rank.totalMessages.get() + scs.stream()
                                    .map(sc -> new Long(sc.getMessagesNotSentCount()))
                                    .reduce(new Long(0), (v1, v2) -> new Long(v1.longValue() + v2.longValue()).longValue()))));

                    // assert that at least SOMETHING went through
                    assertTrue(rank.totalMessages.get() > 0);

                    LOGGER.info("testForProfiler time " + (System.currentTimeMillis() - startTime));

                    @SuppressWarnings("unchecked")
                    final AtomicLong count = nodes.stream()
                            .map(nmwc -> (MessageProcessor<NumberCounter>) nmwc.manager.getMp("test-cluster1")) // get the NumberCounter Mp
                            .filter(l -> l != null) // if it exists
                            .map(l -> l.getPrototype().messageCount) // pull the prototype
                            .reduce(new AtomicLong(0), (v1, v2) -> new AtomicLong(v1.get() + v2.get())); // sum up all of the counts

                    assertEquals(profilerTestNumberCount, count.get());

                } finally {
                    LOGGER.info("=====================================================================================");
                }
            });
        } catch (final Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    final static KeyExtractor ke = new KeyExtractor();

    private static void runACycle(final AtomicBoolean keepGoing, final int rankIndex, final NumberRank rank, final Runnable sendMessages)
            throws InterruptedException {
        keepGoing.set(true);
        final Thread tmpThread = chain(new Thread(sendMessages, "Thread-testNumberCountDropOneAndReAdd-data-pump"), t -> t.start());
        // wait for the messages to get all the way through
        assertTrue(poll(rank, r -> r.countMap.get()[rankIndex].size() == 20));
        keepGoing.set(false);

        // wait for the thread to exit.
        assertTrue(poll(tmpThread, t -> !t.isAlive()));
    }

    @Test
    public void testNumberCountDropOneAndReAdd() throws Throwable {

        runCombos("testNumberCountDropOneAndReAdd", (r, c, s, t, ser) -> elasticRouterIds.contains(r), actxPath, ns -> {
            // keepGoing is for the separate thread that pumps messages into the system.
            final AtomicBoolean keepGoing = new AtomicBoolean(true);
            try {
                LOGGER.trace("==== <- Starting");

                final List<NodeManagerWithContext> nodes = new ArrayList<>(ns.nodes);

                // grab the adaptor from the 0'th cluster + the 0'th (only) node.
                final NumberProducer adaptor = nodes.get(0).ctx.getBean(NumberProducer.class);

                // grab access to the Dispatcher from the Adaptor
                final Dispatcher dispatcher = adaptor.dispatcher;

                // This is a Runnable that will pump messages to the dispatcher until keepGoing is
                // flipped to 'false.' It's stateless so it can be reused as needed.
                final AtomicInteger rankIndexToSend = new AtomicInteger(0);
                final Runnable sendMessages = () -> {
                    // send a few numbers. There are 20 shards so in order to cover all
                    // shards we can send in 20 messages. It just so happens that the hashCode
                    // for an integer is the integer itself so we can get every shard by sending
                    while (keepGoing.get()) {
                        for (int num = 0; num < 20; num++) {
                            final int number = num;
                            dispatcher.dispatch(uncheck(() -> ke.extract(new Number(number, rankIndexToSend.get()))));
                        }
                    }
                };

                try (final ClusterInfoSession session = ns.sessionFactory.createSession();) {
                    waitForEvenShardDistribution(session, "test-cluster1", 3, ns.nodes);

                    // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
                    final NumberRank rank = nodes.get(4).ctx.getBean(NumberRank.class);

                    runACycle(keepGoing, rankIndexToSend.get(), rank, sendMessages);

                    // now kill a node.
                    final NodeManagerWithContext nm = nodes.remove(2);
                    LOGGER.trace("==== Stopping middle node servicing shards ");
                    nm.manager.stop();
                    waitForEvenShardDistribution(session, "test-cluster1", 2, nodes);
                    LOGGER.trace("==== Stopped middle Dempsy");

                    // make sure everything still goes through
                    runACycle(keepGoing, rankIndexToSend.incrementAndGet(), rank, sendMessages);

                    // now, bring online another instance.
                    LOGGER.trace("==== starting a new one");
                    nodes.add(makeNode(new String[] { "elasticity/mp-num-count.xml" }));
                    waitForEvenShardDistribution(session, "test-cluster1", 3, nodes);

                    // make sure everything still goes through
                    runACycle(keepGoing, rankIndexToSend.incrementAndGet(), rank, sendMessages);
                }
            } finally {
                keepGoing.set(false);
                LOGGER.trace("==== Exiting test.");
            }
        });
    }

    @Test
    public void testNumberCountAddOneThenDrop() throws Throwable {
        runCombos("testNumberCountAddOneThenDrop", (r, c, s, t, ser) -> elasticRouterIds.contains(r), actxPath, ns -> {
            // keepGoing is for the separate thread that pumps messages into the system.
            final AtomicBoolean keepGoing = new AtomicBoolean(true);
            try {
                LOGGER.trace("==== <- Starting");

                final List<NodeManagerWithContext> nodes = new ArrayList<>(ns.nodes);

                // grab the adaptor from the 0'th cluster + the 0'th (only) node.
                final NumberProducer adaptor = nodes.get(0).ctx.getBean(NumberProducer.class);

                // grab access to the Dispatcher from the Adaptor
                final Dispatcher dispatcher = adaptor.dispatcher;

                // This is a Runnable that will pump messages to the dispatcher until keepGoing is
                // flipped to 'false.' It's stateless so it can be reused as needed.
                final AtomicInteger rankIndexToSend = new AtomicInteger(0);
                final Runnable sendMessages = () -> {
                    // send a few numbers. There are 20 shards so in order to cover all
                    // shards we can send in 20 messages. It just so happens that the hashCode
                    // for an integer is the integer itself so we can get every shard by sending
                    while (keepGoing.get()) {
                        for (int num = 0; num < 20; num++) {
                            final int number = num;
                            dispatcher.dispatch(uncheck(() -> ke.extract(new Number(number, rankIndexToSend.get()))));
                        }
                    }
                };

                try (final ClusterInfoSession session = ns.sessionFactory.createSession();) {
                    waitForEvenShardDistribution(session, "test-cluster1", 3, nodes);

                    // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
                    final NumberRank rank = nodes.get(4).ctx.getBean(NumberRank.class);

                    runACycle(keepGoing, rankIndexToSend.get(), rank, sendMessages);

                    // now, bring online another instance.
                    LOGGER.trace("==== starting a new one");
                    nodes.add(makeNode(new String[] { "elasticity/mp-num-count.xml" }));
                    waitForEvenShardDistribution(session, "test-cluster1", 4, nodes);

                    // make sure everything still goes through
                    runACycle(keepGoing, rankIndexToSend.incrementAndGet(), rank, sendMessages);

                    // now kill a node.
                    final NodeManagerWithContext nm = nodes.remove(2);
                    LOGGER.trace("==== Stopping middle node servicing shards ");
                    nm.manager.stop();
                    waitForEvenShardDistribution(session, "test-cluster1", 3, nodes);
                    LOGGER.trace("==== Stopped middle Dempsy");

                    // make sure everything still goes through
                    runACycle(keepGoing, rankIndexToSend.incrementAndGet(), rank, sendMessages);
                }
            } finally {
                keepGoing.set(false);
                LOGGER.trace("==== Exiting test.");
            }
        });
    }

    @Test
    public void testExpansionPassivation() throws Exception {
        final String[][] actxPath = {
                { "elasticity/adaptor.xml", },
                { "elasticity/mp-num-count.xml", },
                { "elasticity/mp-num-rank.xml", },
        };

        runCombos("testExpansionPassivation", (r, c, s, t, ser) -> elasticRouterIds.contains(r), actxPath, ns -> {
            final AtomicBoolean keepGoing = new AtomicBoolean(true);
            try {
                final List<NodeManagerWithContext> nodes = new ArrayList<>(ns.nodes);

                // grab the adaptor from the 0'th cluster + the 0'th (only) node.
                final NumberProducer adaptor = nodes.get(0).ctx.getBean(NumberProducer.class);

                // grab access to the Dispatcher from the Adaptor
                final Dispatcher dispatcher = adaptor.dispatcher;

                final AtomicInteger rankIndexToSend = new AtomicInteger(0);
                final Runnable sendMessages = () -> {
                    // send a few numbers. There are 20 shards so in order to cover all
                    // shards we can send in 20 messages. It just so happens that the hashCode
                    // for an integer is the integer itself so we can get every shard by sending
                    while (keepGoing.get()) {
                        for (int num = 0; num < 20; num++) {
                            final int number = num;
                            dispatcher.dispatch(uncheck(() -> ke.extract(new Number(number, rankIndexToSend.get()))));
                        }
                    }
                };

                try (final ClusterInfoSession session = ns.sessionFactory.createSession();) {
                    waitForEvenShardDistribution(session, "test-cluster1", 1, nodes);

                    final NumberRank rank = nodes.get(2).ctx.getBean(NumberRank.class);

                    runACycle(keepGoing, rankIndexToSend.get(), rank, sendMessages);

                    // now we have 20 Mps in test-cluster1
                    final ClusterMetricGetters sc = (ClusterMetricGetters) nodes.get(1).manager
                            .getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1"));

                    assertEquals(20L, sc.getMessageProcessorCount());

                    // add a second node for the cluster test-cluster1
                    nodes.add(makeNode(new String[] { "elasticity/mp-num-count.xml" }));
                    waitForEvenShardDistribution(session, "test-cluster1", 2, nodes);

                    // about 1/2 should drop out.
                    assertTrue(poll(o -> sc.getMessageProcessorCount() < 15L));
                }

            } finally {
                keepGoing.set(false);
            }

        });
    }
}
