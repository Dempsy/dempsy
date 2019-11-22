package net.dempsy;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.netty.util.internal.ConcurrentSet;
import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.lifecycle.annotation.AbstractResource;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.ResourceManager;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.util.SystemPropertyManager;

public class TestResourceManagement extends DempsyBaseTest {
    private static Logger LOGGER = LoggerFactory.getLogger(TestResourceManagement.class);

    public static final String wordResource = "word-count/AV1611Bible.txt.gz";

    public static String readBible() throws IOException {
        final InputStream is = new GZIPInputStream(new BufferedInputStream(WordProducer.class.getClassLoader().getResourceAsStream(wordResource)));
        final StringWriter writer = new StringWriter();
        IOUtils.copy(is, writer);
        return writer.toString();
    }

    public TestResourceManagement(final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
        final String threadingModelDescription, final Function<String, ThreadingModel> threadingModelSource) {
        super(LOGGER, routerId, containerId, sessCtx, tpid, serType, threadingModelDescription, threadingModelSource);
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
    public static class Word extends AbstractResource implements Serializable {
        private static final long serialVersionUID = 1L;
        private String word;
        public boolean amIOpen = true;

        public static Set<Word> allWords = new ConcurrentSet<Word>();

        public Word() {} // needed for kryo-serializer

        public Word(final String word) {
            this.word = word;
            allWords.add(this);
        }

        @MessageKey
        public String getWord() {
            return word;
        }

        @Override
        public void freeResources() {
            amIOpen = false;
        }

        @Override
        public String toString() {
            return "[ " + word + " ]";
        }
    }

    @MessageType
    public static class WordCount extends AbstractResource implements Serializable {
        private static final long serialVersionUID = 1L;
        public String word;
        public long count;
        public boolean amIOpen = true;

        public static Set<WordCount> allWordCounts = new ConcurrentSet<WordCount>();

        public WordCount(final Word word, final long count) {
            this.word = word.getWord();
            this.count = count;
            allWordCounts.add(this);
        }

        public WordCount() {} // for kryo

        static final Integer one = Integer.valueOf(1);

        @MessageKey
        public String getKey() {
            return word;
        }

        @Override
        public void freeResources() {
            amIOpen = false;
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

        public static boolean useResourceManager = true;

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
            final ResourceManager resourceManager = useResourceManager ? new ResourceManager() : null;
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
                    try (Word word = new Word(wordString);) {
                        if(ResourceManager.first == null)
                            ResourceManager.first = word;
                        dispatcher.dispatchAnnotated(word, resourceManager);
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
            return (WordCounter)super.clone();
        }
    }

    // ========================================================================

    private static void clear() {
        Word.allWords.clear();
        WordCount.allWordCounts.clear();
    }

    @Test
    public void testSimlpeWordCount() throws Throwable {
        WordProducer.useResourceManager = true;

        runSimlpeWordCount("testSimlpeWordCount");
    }

    @Test
    public void testSimlpeWordCountSeparateNodes() throws Throwable {
        WordProducer.useResourceManager = true;

        runSimlpeWordCountSeparateNodes("testSimlpeWordCountSeparateNodes");
    }

    @Test
    public void testSimlpeWordCountNoResourceManager() throws Throwable {
        WordProducer.useResourceManager = false;

        runSimlpeWordCount("testSimlpeWordCountNoResourceManager");
    }

    private void runSimlpeWordCount(final String testName) throws Throwable {
        executeSimpleWordCountTest(new String[][] {{
            "classpath:/word-count-resource/adaptor-kjv.xml",
            "classpath:/word-count-resource/mp-word-count.xml",
        }}, testName);
    }

    private void runSimlpeWordCountSeparateNodes(final String testName) throws Throwable {
        executeSimpleWordCountTest(new String[][] {
            {"classpath:/word-count-resource/adaptor-kjv.xml"},
            {"classpath:/word-count-resource/mp-word-count.xml"}
        }, testName);
    }

    private void executeSimpleWordCountTest(final String[][] ctxs, final String testName) throws Throwable {
        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager().set("min_nodes", "1")) {
            WordProducer.latch = new CountDownLatch(1); // need to make it wait.
            runCombos(testName, ctxs, n -> {
                clear();
                try {
                    final List<NodeManagerWithContext> nodes = n.nodes;
                    final NodeManager manager1 = nodes.get(0).manager;

                    // wait until I can reach the cluster from the adaptor.
                    assertTrue(poll(o -> manager1.getRouter().allReachable("test-cluster1").size() == 1));

                    final ClassPathXmlApplicationContext ctx = nodes.get(0).ctx;

                    final WordProducer adaptor;
                    final ClusterMetricGetters stats;

                    WordProducer.latch.countDown();

                    adaptor = ctx.getBean(WordProducer.class);
                    // somtimes there's 1 node, sometimes there's 2
                    final NodeManager manager2 = nodes.size() > 1 ? nodes.get(1).manager : manager1;
                    stats = (ClusterMetricGetters)manager2.getClusterStatsCollector(new ClusterId(currentAppName, "test-cluster1"));

                    assertTrue(poll(o -> adaptor.done.get()));
                    assertTrue(poll(o -> {
                        // System.out.println("" + adaptor.numDispatched + " == " + stats.getProcessedMessageCount());
                        return adaptor.numDispatched == stats.getProcessedMessageCount() + stats.getMessageDiscardedCount();
                    }));

                    // check that all of the Word instances have been freed
                    Word.allWords.forEach(w -> assertFalse(w.amIOpen));
                    Word.allWords.forEach(w -> assertEquals(0, w.refCount()));
                    WordCount.allWordCounts.forEach(w -> assertFalse(w.amIOpen));
                    WordCount.allWordCounts.forEach(w -> assertEquals(0, w.refCount()));
                } finally {
                    clear();
                }
            });
        }

    }

}
