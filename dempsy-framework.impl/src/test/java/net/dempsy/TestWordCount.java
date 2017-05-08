package net.dempsy;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.utils.test.SystemPropertyManager;

public class TestWordCount extends DempsyBaseTest {
    private static Logger LOGGER = LoggerFactory.getLogger(TestWordCount.class);

    public static final String wordResource = "word-count/AV1611Bible.txt.gz";

    public static String readBible() throws IOException {
        final InputStream is = new GZIPInputStream(new BufferedInputStream(WordProducer.class.getClassLoader().getResourceAsStream(wordResource)));
        final StringWriter writer = new StringWriter();
        IOUtils.copy(is, writer);
        return writer.toString();
    }

    public TestWordCount(final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
            final String threadingModelDescription, final Function<String, ThreadingModel> threadingModelSource) {
        super(LOGGER, routerId, containerId, sessCtx, tpid, serType, threadingModelSource);
    }

    @Before
    public void setup() {
        WordProducer.latch = new CountDownLatch(0);
    }

    @AfterClass
    public static void cleanup() {
        WordProducer.strings = null;
        LOGGER.debug("cleaned up");
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

        static final Integer one = new Integer(1);

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

        private static String[] strings;

        static {
            try {
                setupStream();
            } catch (final Throwable e) {
                LOGGER.error("Failed to load source data", e);
            }
        }

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {
            try {
                try {
                    latch.await();
                } catch (final InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                isRunning.set(true);
                while (isRunning.get() && !done.get()) {
                    // obtain data from an external source
                    final String wordString = getNextWordFromSoucre();
                    try {
                        dispatcher.dispatch(ke.extract(new Word(wordString)));
                        numDispatched++;
                        if (numDispatched % 10000 == 0)
                            System.out.print(".");
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
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

            while (!stopped.get())
                Thread.yield();
        }

        private int curCount = 0;

        private String getNextWordFromSoucre() {
            final String ret = strings[curCount++];
            if (curCount >= strings.length) {
                if (onePass)
                    done.set(true);
                curCount = 0;
            }

            return ret;
        }

        private synchronized static void setupStream() throws IOException {
            if (strings == null) {
                strings = readBible().split("\\s+");
            }
        }
    }

    @Mp
    public static class WordCounter implements Cloneable {
        long counter = 0;
        String wordText;

        @Activation
        public void initMe(final String key) {
            this.wordText = key;
        }

        @MessageHandler
        public WordCount handle(final Word word) {
            return new WordCount(word, counter++);
        }

        @Override
        public WordCounter clone() throws CloneNotSupportedException {
            return (WordCounter) super.clone();
        }
    }

    public static class Rank {
        public final String word;
        public final Long rank;

        public Rank(final String work, final long rank) {
            this.word = work;
            this.rank = rank;
        }

        @Override
        public String toString() {
            return "[ " + word + " count:" + rank + " ]";
        }
    }

    @Mp
    public static class WordRank implements Cloneable {
        // This map is shared among clones.
        final public Map<String, Long> countMap = new ConcurrentHashMap<String, Long>();

        @MessageHandler
        public void handle(final WordCount wordCount) {
            countMap.put(wordCount.word, wordCount.count);
        }

        @Override
        public WordRank clone() throws CloneNotSupportedException {
            return (WordRank) super.clone();
        }

        public List<Rank> getPairs() {
            final List<Rank> ret = new ArrayList<>(countMap.size() + 10);
            for (final Map.Entry<String, Long> cur : countMap.entrySet())
                ret.add(new Rank(cur.getKey(), cur.getValue()));
            Collections.sort(ret, (o1, o2) -> o2.rank.compareTo(o1.rank));
            return ret;
        }
    }

    // ========================================================================

    Set<String> finalResults = new HashSet<String>();

    {
        finalResults.addAll(Arrays.asList("the", "and", "of", "to", "And", "in", "that", "he", "shall", "unto", "I"));
    }

    @Test
    public void testWordCountNoRank() throws Throwable {
        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "1")) {
            final String[][] ctxs = { {
                    "classpath:/word-count/adaptor-kjv.xml",
                    "classpath:/word-count/mp-word-count.xml",
            } };

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRank", ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager manager = nodes.get(0).manager;

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager.getRouter().allReachable("test-cluster1").size() == 1));

                final ClassPathXmlApplicationContext ctx = nodes.get(0).ctx;

                final WordProducer adaptor;
                final ClusterMetricGetters stats;

                WordProducer.latch.countDown();

                adaptor = ctx.getBean(WordProducer.class);
                stats = (ClusterMetricGetters) manager.getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1"));

                assertTrue(poll(o -> adaptor.done.get()));
                assertTrue(poll(o -> {
                    // System.out.println("" + adaptor.numDispatched + " == " + stats.getProcessedMessageCount());
                    return adaptor.numDispatched == stats.getProcessedMessageCount();
                }));
            });
        }
    }

    @Test
    public void testWordCountWithRank() throws Throwable {
        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "1")) {

            final String[][] ctxs = { {
                    "classpath:/word-count/adaptor-kjv.xml",
                    "classpath:/word-count/mp-word-count.xml",
                    "classpath:/word-count/mp-word-rank.xml",
            } };

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountWithRank", ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager manager = nodes.get(0).manager;

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager.getRouter().allReachable("test-cluster1").size() == 1));

                final ClassPathXmlApplicationContext ctx = nodes.get(0).ctx;

                final WordProducer adaptor;
                final ClusterMetricGetters stats;

                WordProducer.latch.countDown();

                adaptor = ctx.getBean(WordProducer.class);
                stats = (ClusterMetricGetters) manager.getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1"));

                assertTrue(poll(o -> adaptor.done.get()));
                assertTrue(poll(o -> adaptor.numDispatched == stats.getProcessedMessageCount()));

                // wait until all of the counts are also passed to WordRank
                final ClusterMetricGetters wrStats = (ClusterMetricGetters) manager
                        .getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster2"));
                assertTrue(poll(wrStats, s -> adaptor.numDispatched == s.getProcessedMessageCount()));

                stopSystem();

                // pull the Rank mp from the manager
                final MessageProcessorLifecycle<?> mp = AccessUtil.getMp(manager, "test-cluster2");
                @SuppressWarnings("unchecked")
                final WordRank prototype = ((MessageProcessor<WordRank>) mp).getPrototype();
                final List<Rank> ranks = prototype.getPairs();
                Collections.sort(ranks, (o1, o2) -> o2.rank.compareTo(o1.rank));

                final List<Rank> top10 = ranks.subList(0, 10);
                top10.forEach(r -> assertTrue(finalResults.contains(r.word)));
            });
        }
    }

    public boolean waitForAllSent(final WordProducer adaptor) throws InterruptedException {
        // as long as it's progressing we keep waiting.
        int previous = -1;
        int next = adaptor.numDispatched;
        while (next > previous && !adaptor.done.get()) {
            if (poll(1000, o -> adaptor.done.get()))
                break;
            previous = next;
            next = adaptor.numDispatched;
        }
        return poll(o -> adaptor.done.get());
    }

    @Test
    public void testWordCountNoRankMultinode() throws Throwable {
        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "2")) {

            final String[][] ctxs = {
                    { "classpath:/word-count/adaptor-kjv.xml", "classpath:/word-count/mp-word-count.xml", },
                    { "classpath:/word-count/mp-word-count.xml", },
            };

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRankMultinode", (r, c, s, t, ser) -> !r.equals("simple"), ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] manager = Arrays.asList(nodes.get(0).manager, nodes.get(1).manager).toArray(new NodeManager[2]);
                final ClassPathXmlApplicationContext[] ctx = Arrays.asList(nodes.get(0).ctx, nodes.get(1).ctx)
                        .toArray(new ClassPathXmlApplicationContext[2]);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> manager[0].getRouter().allReachable("test-cluster1").size() == 2));
                assertTrue(poll(o -> manager[1].getRouter().allReachable("test-cluster1").size() == 2));

                WordProducer.latch.countDown();

                final WordProducer adaptor = ctx[0].getBean(WordProducer.class);
                final ClusterMetricGetters[] stats = Arrays.asList(
                        (ClusterMetricGetters) manager[0].getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1")),
                        (ClusterMetricGetters) manager[1].getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1")))
                        .toArray(new ClusterMetricGetters[2]);

                assertTrue(waitForAllSent(adaptor));
                assertTrue(poll(o -> {
                    // System.out.println(stats[0].getProcessedMessageCount() + ", " + stats[1].getProcessedMessageCount());
                    return adaptor.numDispatched == Arrays.stream(stats).map(c -> c.getProcessedMessageCount())
                            .reduce((c1, c2) -> c1.longValue() + c2.longValue()).get().longValue();
                }));
            });
        }
    }

    @Test
    public void testWordCountNoRankAdaptorOnlyNode() throws Throwable {
        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "2")) {

            final String[][] ctxs = {
                    { "classpath:/word-count/adaptor-kjv.xml", }, // adaptor only node
                    { "classpath:/word-count/mp-word-count.xml", },
                    { "classpath:/word-count/mp-word-count.xml", },
                    { "classpath:/word-count/mp-word-count.xml", },
                    { "classpath:/word-count/mp-word-count.xml", },
                    { "classpath:/word-count/mp-word-count.xml", },
            };

            final int NUM_WC = ctxs.length - 1; // the adaptor is the first one.

            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos("testWordCountNoRankMultinode", (r, c, s, t, ser) -> !r.equals("simple"), ctxs, n -> {
                final List<NodeManagerWithContext> nodes = n.nodes;
                final NodeManager[] managers = nodes.stream().map(nm -> nm.manager).toArray(NodeManager[]::new);

                // wait until I can reach the cluster from the adaptor.
                assertTrue(poll(o -> managers[0].getRouter().allReachable("test-cluster1").size() == NUM_WC));

                WordProducer.latch.countDown();

                final WordProducer adaptor = nodes.get(0).ctx.getBean(WordProducer.class);
                final List<ClusterMetricGetters> stats = Arrays.asList(managers)
                        .subList(1, managers.length)
                        .stream()
                        .map(nm -> nm.getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1")))
                        .map(sc -> (ClusterMetricGetters) sc)
                        .collect(Collectors.toList());

                waitForAllSent(adaptor);
                assertTrue(poll(o -> adaptor.done.get()));
                assertTrue(poll(o -> {
                    // System.out.println(stats[0].getProcessedMessageCount() + ", " + stats[1].getProcessedMessageCount());
                    return adaptor.numDispatched == stats.stream().map(c -> c.getProcessedMessageCount())
                            .reduce((c1, c2) -> c1.longValue() + c2.longValue()).get().longValue();
                }));
            });
        }
    }
}
