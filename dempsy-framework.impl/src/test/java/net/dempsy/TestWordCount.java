package net.dempsy;

import static net.dempsy.utils.test.ConditionPoll.assertTrue;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.netty.util.internal.ConcurrentSet;
import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.container.NodeMetricGetters;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.Passivation;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.util.SystemPropertyManager;

public class TestWordCount extends DempsyBaseTest {
    private static Logger LOGGER = LoggerFactory.getLogger(TestWordCount.class);

    public static final String wordResource = "word-count/AV1611Bible.txt.gz";

    // This is because strict comparisons of message emitted when there is a very small limit
    // on the container queuing under a fast (in-process) asynchronous (i.e. BlockingQueue)
    // transport can't guarantee predictable results. This is set to 'false' in this child test
    // TestWordCountLimitedContainerQueue
    protected boolean strict = true;
    protected boolean considerOnlyContainersThatCanLimit = false;

    public static String readBible() throws IOException {
        try(final InputStream is = new GZIPInputStream(new BufferedInputStream(WordProducer.class.getClassLoader().getResourceAsStream(wordResource)));
            final StringWriter writer = new StringWriter();) {
            IOUtils.copy(is, writer, StandardCharsets.UTF_8);
            return writer.toString();
        }
    }

    public TestWordCount(final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
        final String threadingModelDescription, final Function<String, ThreadingModel> threadingModelSource) {
        super(LOGGER, routerId, containerId, sessCtx, tpid, serType, threadingModelDescription, threadingModelSource);
    }

    protected TestWordCount(final Logger logger, final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
        final String threadingModelDescription, final Function<String, ThreadingModel> threadingModelSource, final boolean considerOnlyContainersThatCanLimit) {
        super(logger, routerId, containerId, sessCtx, tpid, serType, threadingModelDescription, threadingModelSource);
        this.considerOnlyContainersThatCanLimit = considerOnlyContainersThatCanLimit;
    }

    @Before
    public void setup() {
        WordProducer.latch = new CountDownLatch(0);
    }

    // ========================================================================
    // Test classes we will be working with. The old word count example.
    // ========================================================================
    @MessageType
    public static class Word implements Serializable {
        private static final long serialVersionUID = 1L;
        private String word;

        public Word() {} // needed for kryo-serializer

        public Word(final String word) {
            this.word = word;
        }

        @MessageKey
        public String getWord() {
            return word;
        }

        @Override
        public String toString() {
            return "[ " + word + " ]";
        }
    }

    @MessageType
    public static class WordCount implements Serializable {
        private static final long serialVersionUID = 1L;
        public String word;
        public long count;

        public WordCount(final Word word, final long count) {
            this.word = word.getWord();
            this.count = count;
        }

        public WordCount() {} // for kryo

        static final Integer one = Integer.valueOf(1);

        @MessageKey
        public String getKey() {
            return word;
        }

        @Override
        public String toString() {
            return "[ " + word + ", " + count + " ]";
        }
    }

    public static class WordProducer implements Adaptor {
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private Dispatcher dispatcher = null;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        public boolean onePass = true;
        public static CountDownLatch latch = new CountDownLatch(0);
        KeyExtractor ke = new KeyExtractor();
        int numDispatched = 0;
        Thread runningThread = null;

        private static String[] strings;

        static {
            try {
                setupStream();
            } catch(final Throwable e) {
                LOGGER.error("Failed to load source data", e);
            }
        }

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {
            runningThread = Thread.currentThread();
            try {
                try {
                    latch.await();
                } catch(final InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                isRunning.set(true);
                while(isRunning.get() && !done.get()) {
                    // obtain data from an external source
                    final String wordString = getNextWordFromSoucre();
                    try {
                        dispatcher.dispatch(ke.extract(new Word(wordString)));
                        numDispatched++;
                        if(numDispatched % 10000 == 0)
                            System.out.print(".");
                    } catch(IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                        LOGGER.error("Failed to dispatch", e);
                    }
                }
                System.out.println();
            } finally {
                stopped.set(true);
            }
        }

        @Override
        public void stop() {
            isRunning.set(false);

            while(!stopped.get()) {
                Thread.yield();
                if(runningThread != null)
                    runningThread.interrupt();
                else
                    break; // runningThread was never set so start might not have been called.
            }
        }

        private int curCount = 0;

        private String getNextWordFromSoucre() {
            final String ret = strings[curCount++];
            if(curCount >= strings.length) {
                if(onePass)
                    done.set(true);
                curCount = 0;
            }

            return ret;
        }

        private synchronized static void setupStream() throws IOException {
            if(strings == null) {
                strings = readBible().split("\\s+");
            }
        }
    }

    @Mp
    public static class WordCounter implements Cloneable {
        static Set<String> aliveWordsToTestPassivate = new ConcurrentSet<String>();
        long counter = 0;
        String wordText;

        @Activation
        public void initMe(final String key) {
            this.wordText = key;
            aliveWordsToTestPassivate.add(key);
        }

        @Passivation
        public void close() {
            aliveWordsToTestPassivate.remove(wordText);
        }

        @MessageHandler
        public WordCount handle(final Word word) {
            return new WordCount(word, counter++);
        }

        @Override
        public WordCounter clone() throws CloneNotSupportedException {
            return (WordCounter)super.clone();
        }
    }

    static final Long dumbKey = Long.valueOf(0L);

    @MessageType
    public static class Rank implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String word;
        public final Long rank;

        @SuppressWarnings("unused")
        private Rank() {
            word = null;
            rank = null;
        }

        public Rank(final String word, final long rank) {
            this.word = word;
            this.rank = rank;
        }

        @MessageKey
        public Long dumbKey() {
            return dumbKey;
        }

        @Override
        public String toString() {
            return "[ " + word + " count:" + rank + " ]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((rank == null) ? 0 : rank.hashCode());
            result = prime * result + ((word == null) ? 0 : word.hashCode());
            return result;
        }

        // ignores any possible nulls
        @Override
        public boolean equals(final Object obj) {
            final Rank other = (Rank)obj;
            if(!rank.equals(other.rank))
                return false;
            if(!word.equals(other.word))
                return false;
            return true;
        }
    }

    @Mp
    public static class WordRank implements Cloneable {
        // This map is shared among clones.
        final public Map<String, Long> countMap = new ConcurrentHashMap<>();

        @MessageHandler
        public void handle(final WordCount wordCount) {
            countMap.put(wordCount.word, wordCount.count);
        }

        @Override
        public WordRank clone() throws CloneNotSupportedException {
            return (WordRank)super.clone();
        }

        public List<Rank> getPairs() {
            final List<Rank> ret = new ArrayList<>(countMap.size() + 10);
            for(final Map.Entry<String, Long> cur: countMap.entrySet())
                ret.add(new Rank(cur.getKey(), cur.getValue()));
            Collections.sort(ret, (o1, o2) -> o2.rank.compareTo(o1.rank));
            return ret;
        }
    }

    @Mp
    public static class WordCountLastOutput implements Cloneable {
        protected WordCount last = null;

        @MessageHandler
        public void handle(final WordCount wordCount) {
            // keep the largest count only.
            if(last == null || last.count < wordCount.count)
                last = wordCount;
        }

        @Override
        public WordCountLastOutput clone() throws CloneNotSupportedException {
            return (WordCountLastOutput)super.clone();
        }

        @Output
        public Rank emit() {
            return (last != null) ? new Rank(last.word, last.count) : null;
        }
    }

    @Mp
    public static class RankCatcher implements Cloneable {
        // This list is shared among clones - though there should be only 1.
        private final AtomicReference<List<Rank>> topRef = new AtomicReference<>(new ArrayList<>());
        private long minInList = -1;

        @Override
        public RankCatcher clone() throws CloneNotSupportedException {
            return (RankCatcher)super.clone();
        }

        @MessageHandler
        public void handle(final Rank rank) {
            if(rank.rank.longValue() < minInList)
                return;

            // is this ranking on top?
            final ArrayList<Rank> newList = new ArrayList<>();
            boolean takenCareOf = false;
            final List<Rank> top = topRef.get();
            for(final Rank cur: top) {
                if(cur.word.equals(rank.word)) {
                    // which one goes in the list?
                    newList.add(cur.rank > rank.rank ? cur : rank);
                    takenCareOf = true;
                } else
                    newList.add(cur);
            }

            if(!takenCareOf)
                newList.add(rank);

            Collections.sort(newList, (o1, o2) -> {
                return o2.rank.compareTo(o1.rank);
            });

            while(newList.size() > 10)
                newList.remove(newList.size() - 1);

            minInList = newList.get(newList.size() - 1).rank.longValue();

            if(!top.equals(newList))
                topRef.set(newList);
        }
    }

    // ========================================================================

    Set<String> finalResults = new HashSet<>();

    {
        finalResults.addAll(Arrays.asList("the", "and", "of", "to", "And", "in", "that", "he", "shall", "unto", "I", "his"));
    }

    private boolean isContainerOkay(final String container) {
        if(considerOnlyContainersThatCanLimit)
            return containerSupportsLimitedQueueLen(container);
        else
            return true;
    }

    public boolean waitForAllSent(final WordProducer adaptor) throws InterruptedException {
        // as long as it's progressing we keep waiting.
        int previous = -1;
        int next = adaptor.numDispatched;
        while(next > previous && !adaptor.done.get()) {
            if(poll(1000, o -> adaptor.done.get()))
                break;
            previous = next;
            next = adaptor.numDispatched;
        }
        return poll(o -> adaptor.done.get());
    }

    // assumption here is that the adaptor is in a separate node
    // and every OTHER node contains a node stats collector + a series of
    // cluster stats collectors all in a Tuple.
    private int allDiscardedMessagesCount(final NodeMetricGetters adaptorStats, final List<Tuple> nodeStats) {
        return nodeStats.stream()
            .mapToLong(s -> ((NodeMetricGetters)s.getValue(0)).getDiscardedMessageCount()
                + s.toList().subList(1, s.getSize()).stream()
                    .map(o -> (ClusterMetricGetters)o)
                    .mapToLong(cm -> cm.getMessageDiscardedCount())
                    .sum())
            .mapToInt(l -> (int)l)
            .sum()

            + Optional.ofNullable(adaptorStats).map(a -> a.getMessagesNotSentCount()).orElse(0L).intValue();
    }

    @SuppressWarnings("unchecked")
    private <A, B, C> List<Tuple> castToTupleList3(final List<Triplet<A, B, C>> t) {
        return (List<Tuple>)(Object)t;
    }

    @SuppressWarnings("unchecked")
    private <A, B> List<Tuple> castToTupleList2(final List<Pair<A, B>> t) {
        return (List<Tuple>)(Object)t;
    }

    // ========================================================================

    @Test
    public void testWordCountNoRank() throws Throwable {
        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "1")) {
            final String[][] ctxs = {{
                "classpath:/word-count/adaptor-kjv.xml",
                "classpath:/word-count/mp-word-count.xml",
            }};

            WordCounter.aliveWordsToTestPassivate.clear();

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRank", (r, c, s, t, ser) -> isContainerOkay(c), ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager manager = nodes.get(0).manager;

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager.getRouter().allReachable("test-cluster1").size() == 1));

                final ClassPathXmlApplicationContext ctx = nodes.get(0).ctx;

                final WordProducer adaptor;
                final ClusterMetricGetters stats;

                WordProducer.latch.countDown();

                adaptor = ctx.getBean(WordProducer.class);
                stats = (ClusterMetricGetters)manager.getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1"));

                assertTrue(poll(o -> adaptor.done.get()));

                assertTrue(poll(o -> {
                    // System.out.println("" + adaptor.numDispatched + " == " + stats.getProcessedMessageCount());
                    return adaptor.numDispatched == (stats.getProcessedMessageCount() + stats.getMessageDiscardedCount());
                }));
            });

            final int remaining = WordCounter.aliveWordsToTestPassivate.size();
            WordCounter.aliveWordsToTestPassivate.clear();
            assertEquals(0, remaining);

        }
    }

    @Test
    public void testWordCountWithRank() throws Throwable {
        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "1")) {

            final String[][] ctxs = {{
                "classpath:/word-count/adaptor-kjv.xml",
                "classpath:/word-count/mp-word-count.xml",
                "classpath:/word-count/mp-word-rank.xml",
            }};

            WordCounter.aliveWordsToTestPassivate.clear();

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountWithRank", (r, c, s, t, ser) -> isContainerOkay(c), ctxs, n -> {
                final ClusterId countCId = new ClusterId(currentAppName, "test-cluster1");
                final ClusterId rankCId = new ClusterId(currentAppName, "test-cluster2");

                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager manager = nodes.get(0).manager;

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager.getRouter().allReachable("test-cluster1").size() == 1));

                final ClassPathXmlApplicationContext ctx = nodes.get(0).ctx;

                final WordProducer adaptor;

                WordProducer.latch.countDown();

                adaptor = ctx.getBean(WordProducer.class);

                assertTrue(poll(o -> adaptor.done.get()));

                final var statsByNode = Triplet.with((NodeMetricGetters)manager.getNodeStatsCollector(),
                    (ClusterMetricGetters)manager.getClusterStatsCollector(countCId),
                    (ClusterMetricGetters)manager.getClusterStatsCollector(rankCId));

                // wait until all of the counts are also passed to WordRank
                assertTrue(poll(o -> {
                    final int allDiscardedMessagesCount = allDiscardedMessagesCount(null, castToTupleList3(List.of(statsByNode)));
                    final ClusterMetricGetters rstat = statsByNode.getValue2();

                    return adaptor.numDispatched == allDiscardedMessagesCount + rstat.getDispatchedMessageCount();
                }));

                stopSystem();

                // pull the Rank mp from the manager
                final MessageProcessorLifecycle<?> mp = AccessUtil.getMp(manager, "test-cluster2");
                @SuppressWarnings("unchecked")
                final WordRank prototype = ((MessageProcessor<WordRank>)mp).getPrototype();
                final List<Rank> ranks = prototype.getPairs();
                Collections.sort(ranks, (o1, o2) -> o2.rank.compareTo(o1.rank));

                final int top10OrSo = (ranks.size() < 10) ? ranks.size() : 10;
                final List<Rank> top10 = ranks.subList(0, top10OrSo);
                final String errStr = "expected the top 10 results: " + top10 + " to all be in:" + finalResults;
                top10.stream()
                    .forEach(r -> assertTrue(errStr, finalResults.contains(r.word)));
            });

            final int remaining = WordCounter.aliveWordsToTestPassivate.size();
            WordCounter.aliveWordsToTestPassivate.clear();
            assertEquals(0, remaining);
        }
    }

    @Test
    public void testWordCountNoRankMultinode() throws Throwable {
        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "2")) {

            final String[][] ctxs = {
                {"classpath:/word-count/adaptor-kjv.xml","classpath:/word-count/mp-word-count.xml",},
                {"classpath:/word-count/mp-word-count.xml",},
            };

            WordCounter.aliveWordsToTestPassivate.clear();

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRankMultinode", (r, c, s, t, ser) -> isElasticRoutingStrategy(r) && isContainerOkay(c), ctxs, n -> {
                final ClusterId countCId = new ClusterId(currentAppName, "test-cluster1");

                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] manager = Arrays.asList(nodes.get(0).manager, nodes.get(1).manager).toArray(new NodeManager[2]);
                final ClassPathXmlApplicationContext[] ctx = Arrays.asList(nodes.get(0).ctx, nodes.get(1).ctx)
                    .toArray(new ClassPathXmlApplicationContext[2]);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager[0].getRouter().allReachable("test-cluster1").size() == 2));
                assertTrue(poll(o -> manager[1].getRouter().allReachable("test-cluster1").size() == 2));

                WordProducer.latch.countDown();

                final WordProducer adaptor = ctx[0].getBean(WordProducer.class);

                assertTrue(waitForAllSent(adaptor));
                assertTrue(poll(o -> adaptor.done.get()));

                final var statsByNode = Arrays.asList(manager).stream()
                    .map(nm -> Pair.with((NodeMetricGetters)nm.getNodeStatsCollector(),
                        (ClusterMetricGetters)nm.getClusterStatsCollector(countCId)))
                    .collect(Collectors.toList());

                assertTrue(poll(o -> {
                    final int allDiscardedMessagesCount = allDiscardedMessagesCount(null, castToTupleList2(statsByNode));

                    return adaptor.numDispatched == allDiscardedMessagesCount + statsByNode.stream()
                        .mapToInt(sc -> (int)sc.getValue1().getDispatchedMessageCount())
                        .sum();
                }));
            });

            final int remaining = WordCounter.aliveWordsToTestPassivate.size();
            WordCounter.aliveWordsToTestPassivate.clear();
            assertEquals(0, remaining);
        }
    }

    @Test
    public void testWordCountNoRankAdaptorOnlyNode() throws Throwable {
        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "2")) {

            final String[][] ctxs = {
                {"classpath:/word-count/adaptor-kjv.xml",}, // adaptor only node
                {"classpath:/word-count/mp-word-count.xml",},
                {"classpath:/word-count/mp-word-count.xml",},
                {"classpath:/word-count/mp-word-count.xml",},
                {"classpath:/word-count/mp-word-count.xml",},
                {"classpath:/word-count/mp-word-count.xml",},
            };

            final int NUM_WC = ctxs.length - 1; // the adaptor is the first one.

            WordCounter.aliveWordsToTestPassivate.clear();

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRankMultinode", (r, c, s, t, ser) -> isElasticRoutingStrategy(r) && isContainerOkay(c), ctxs, n -> {
                final ClusterId countCId = new ClusterId(currentAppName, "test-cluster1");

                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] managers = nodes.stream().map(nm -> nm.manager).toArray(NodeManager[]::new);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster1").size() == NUM_WC));

                WordProducer.latch.countDown();

                final WordProducer adaptor = nodes.get(0).ctx.getBean(WordProducer.class);

                waitForAllSent(adaptor);
                assertTrue(poll(o -> adaptor.done.get()));

                final var statsByNode = Arrays.asList(managers).subList(1, managers.length).stream()
                    .map(nm -> Pair.with((NodeMetricGetters)nm.getNodeStatsCollector(),
                        (ClusterMetricGetters)nm.getClusterStatsCollector(countCId)))
                    .collect(Collectors.toList());
                final NodeMetricGetters adaptorStats = (NodeMetricGetters)managers[0].getNodeStatsCollector();

                assertTrue(poll(o -> {
                    final int allDiscardedMessagesCount = allDiscardedMessagesCount(adaptorStats, castToTupleList2(statsByNode));

                    return adaptor.numDispatched == allDiscardedMessagesCount + statsByNode.stream()
                        .mapToInt(sc -> (int)sc.getValue1().getDispatchedMessageCount())
                        .sum();
                }));
            });

            final int remaining = WordCounter.aliveWordsToTestPassivate.size();
            WordCounter.aliveWordsToTestPassivate.clear();
            assertEquals(0, remaining);
        }
    }

    @Test
    public void testWordCountHomogeneousProcessing() throws Throwable {
        final String[][] ctxs = {
            {"classpath:/word-count/adaptor-kjv.xml",}, // adaptor only node
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-rank.xml"},
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-rank.xml"},
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-rank.xml"},
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-rank.xml"},
        };

        final int NUM_WC = ctxs.length - 1; // the adaptor is the first one.

        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager()
            .set("min_nodes", Integer.toString(NUM_WC))
            .set("routing-group", ":group")
            .set("send_threads", "1")
            .set("receive_threads", "1")
            .set("blocking-queue-size", "500000")) {

            WordCounter.aliveWordsToTestPassivate.clear();

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountHomogeneousProcessing", (r, c, s, t, ser) -> isElasticRoutingStrategy(r) && isContainerOkay(c), ctxs, n -> {
                final ClusterId countCId = new ClusterId(currentAppName, "test-cluster1");
                final ClusterId rankCId = new ClusterId(currentAppName, "test-cluster2");

                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] managers = nodes.stream().map(nm -> nm.manager).toArray(NodeManager[]::new);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster1").size() == NUM_WC));
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster2").size() == NUM_WC));

                WordProducer.latch.countDown();

                final WordProducer adaptor = nodes.get(0).ctx.getBean(WordProducer.class);
                waitForAllSent(adaptor);

                // get all of the stats collectors.
                final List<Triplet<NodeMetricGetters, ClusterMetricGetters, ClusterMetricGetters>> statsByNode = Arrays.asList(managers)
                    .subList(1, managers.length)
                    .stream()
                    .map(nm -> Triplet.with((NodeMetricGetters)nm.getNodeStatsCollector(),
                        (ClusterMetricGetters)nm.getClusterStatsCollector(countCId),
                        (ClusterMetricGetters)nm.getClusterStatsCollector(rankCId)))
                    .collect(Collectors.toList());
                final NodeMetricGetters adaptorStats = (NodeMetricGetters)managers[0].getNodeStatsCollector();

                final int totalSent = adaptor.numDispatched;
                // now wait for the sum of all messages received by the ranking to be the number sent
                assertTrue(poll(o -> {
                    // System.out.println("" + adaptor.numDispatched + " ==? ");
                    final int allDiscardedMessagesCount = allDiscardedMessagesCount(adaptorStats, castToTupleList3(statsByNode));
                    final int totalRanked = allDiscardedMessagesCount + statsByNode.stream()
                        // .peek(sc -> System.out.print("[" +
                        // adaptorStats.getMessagesNotSentCount() + "," + sc.getValue0().getDiscardedMessageCount() +
                        // ",c:(" + sc.getValue1().getDispatchedMessageCount() + ", " + sc.getValue1().getMessageDiscardedCount() + ")" +
                        // ",r:(" + sc.getValue2().getDispatchedMessageCount() + ", " + sc.getValue2().getMessageDiscardedCount() + ")] "))
                        .mapToInt(sc -> (int)sc.getValue2().getDispatchedMessageCount())
                        .sum();
                    System.out.println(": " + totalRanked + " == " + totalSent);
                    return totalRanked == totalSent;
                }));

                // no nodes (except the adaptor node) should have sent any messages.
                // IOW, messages got to the Rank processor never leaving the node the Count was executed.
                final List<NodeMetricGetters> nodeStats = Arrays.asList(managers)
                    .subList(1, managers.length)
                    .stream()
                    .map(nm -> nm.getNodeStatsCollector())
                    .map(s -> (NodeMetricGetters)s)
                    .collect(Collectors.toList());

                // if the routing id isn't a group id then there should be cross talk.
                assertEquals(NUM_WC, nodeStats.size());
                for(final NodeMetricGetters mg: nodeStats)
                    assertEquals(0, mg.getMessagesNotSentCount());
                if(isGroupRoutingStrategy(routerId)) {
                    for(final NodeMetricGetters mg: nodeStats)
                        assertEquals(0, mg.getMessagesSentCount());
                } else {
                    assertNotNull(nodeStats.stream().filter(mg -> mg.getMessagesSentCount() > 0).findFirst().orElse(null));
                }
            });

            final int remaining = WordCounter.aliveWordsToTestPassivate.size();
            WordCounter.aliveWordsToTestPassivate.clear();
            assertEquals(0, remaining);
        }
    }

    @Test
    public void testWithOutputCycleCron() throws Exception {
        runTestWithOutputCycle("testWithOutputCycleCron", "classpath:/word-count/output-scheduler-cron.xml");
    }

    @Test
    public void testWithOutputCycleRelative() throws Exception {
        runTestWithOutputCycle("testWithOutputCycleRelative", "classpath:/word-count/output-scheduler-relative.xml");
    }

    private void runTestWithOutputCycle(final String testName, final String outputSchedCtx) throws Exception {
        final String[][] ctxs = {
            {"classpath:/word-count/adaptor-kjv.xml",}, // adaptor only node
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-count-output.xml",outputSchedCtx},
            {"classpath:/word-count/mp-word-count.xml","classpath:/word-count/mp-word-count-output.xml",outputSchedCtx},
            {"classpath:/word-count/mp-rank-catcher.xml"},
        };

        final int NUM_WC = ctxs.length - 2; // the adaptor is the first one, rank catcher the last.

        try(@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager()
            .set("min_nodes", Integer.toString(NUM_WC))
            .set("routing-group", ":group")
            .set("send_threads", "1")
            .set("receive_threads", "1")
            .set("blocking-queue-size", "500000")) {

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos(testName, (r, c, s, t, ser) -> isElasticRoutingStrategy(r) && isContainerOkay(c), ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] managers = nodes.stream().map(nm -> nm.manager).toArray(NodeManager[]::new);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster1").size() == NUM_WC));
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster2").size() == NUM_WC));
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster3").size() == 1));

                for(int i = 0; i < NUM_WC; i++) {
                    final int managerIndex = i + 1; // the +1 is because the first (0th) manager is the adaptor
                    assertTrue(poll(o -> managers[managerIndex].getRouter().allReachable("test-cluster3").size() == 1));
                }

                WordProducer.latch.countDown();
                final WordProducer adaptor = nodes.get(0).ctx.getBean(WordProducer.class);
                waitForAllSent(adaptor);

                final NodeManager manager = nodes.get(nodes.size() - 1).manager; // the last node has the RankCatcher
                final MessageProcessorLifecycle<?> mp = AccessUtil.getMp(manager, "test-cluster3");
                @SuppressWarnings("unchecked")
                final RankCatcher prototype = ((MessageProcessor<RankCatcher>)mp).getPrototype();
                if(strict) {
                    final HashSet<String> expected = new HashSet<>(Arrays.asList("the", "that", "unto", "in", "and", "And", "of", "shall", "to", "he"));
                    assertTrue(() -> {
                        return "FAILURE:" + expected.toString() + " != " + prototype.topRef.get();
                    }, poll(prototype.topRef, tr -> {
                        final List<Rank> cur = tr.get();
                        final HashSet<String> topSet = new HashSet<>(cur.stream().map(r -> r.word).collect(Collectors.toSet()));
                        // once more than 1/2 of the list is there then we're done.
                        final int threshold = (expected.size() / 2) + 1;
                        int matches = 0;
                        for(final String exp: expected)
                            if(topSet.contains(exp))
                                matches++;
                        return matches >= threshold;
                    }));
                }
            });
        }
    }
}
