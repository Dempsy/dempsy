/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.container;

import static net.dempsy.AccessUtil.canReach;
import static net.dempsy.AccessUtil.getRouter;
import static net.dempsy.util.Functional.recheck;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;



import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.DempsyException;
import net.dempsy.NodeManager;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.container.Container.Operation;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.container.locking.LockingContainer;
import net.dempsy.container.mocks.ContainerTestMessage;
import net.dempsy.container.mocks.OutputMessage;
import net.dempsy.container.simple.SimpleContainer;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.Evictable;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.Passivation;
import net.dempsy.lifecycle.annotation.Start;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.threading.OrderedPerContainerThreadingModelAlt;
import net.dempsy.transport.blockingqueue.BlockingQueueReceiver;
import net.dempsy.util.SystemPropertyManager;

//
// NOTE: this test simply puts messages on an input queue, and expects
// messages on an output queue; the important part is the wiring
// in TestMPContainer.xml
//
public class TestContainer {
    public static String[] ctx = {
        "classpath:/spring/container/test-container.xml",
        "classpath:/spring/container/test-mp.xml",
        "classpath:/spring/container/test-adaptor.xml"
    };

    public static Stream<Arguments> data() {
        return Stream.of(
            Arguments.of(LockingContainer.class.getPackage().getName()),
            // the NonLockingContainer is broken
            // Arguments.of(NonLockingContainer.class.getPackage().getName()),
            Arguments.of(NonLockingAltContainer.class.getPackage().getName()),
            Arguments.of(SimpleContainer.class.getPackage().getName())
        );
    }

    private Container container = null;

    private ClassPathXmlApplicationContext context = null;
    private NodeManager manager;
    private LocalClusterSessionFactory sessionFactory = null;
    private ClusterStatsCollector statsCollector;

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private String containerId;

    public static Map<String, TestProcessor> cache = null;
    public static Set<OutputMessage> outputMessages = null;
    public static RuntimeException justThrowMe = null;
    public static RuntimeException throwMeInActivation = null;

    private <T extends AutoCloseable> T track(final T o) {
        toClose.add(o);
        return o;
    }

    public void setUp(final String containerId) throws Exception {
        this.containerId = containerId;
        justThrowMe = null;
        throwMeInActivation = null;
        track(new SystemPropertyManager()).set("container-type", containerId);
        context = track(new ClassPathXmlApplicationContext(ctx));
        sessionFactory = new LocalClusterSessionFactory();
        final Node node = context.getBean(Node.class);
        manager = track(new NodeManager()).node(node).collaborator(track(sessionFactory.createSession()));
        if(containerId.contains("simple")) // if it's not thread safe we need an ordered container
            manager.threadingModel(track(new OrderedPerContainerThreadingModelAlt("tm")));
        manager.start();
        statsCollector = manager.getClusterStatsCollector(new ClusterId("test-app", "test-cluster"));
        container = manager.getContainers().get(0);
        assertTrue(poll(manager, m -> m.isReady()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        cache = null;
        outputMessages = null;
        justThrowMe = null;
        throwMeInActivation = null;
        recheck(() -> toClose.forEach(v -> uncheck(() -> v.close())), Exception.class);
        toClose.clear();
        LocalClusterSessionFactory.completeReset();
    }

    @MessageType
    public static class MyMessage {
        @Override
        public String toString() {
            return "MyMessage [value=" + value + "]";
        }

        public String value;

        public MyMessage(final String value) {
            this.value = value;
        }

        @MessageKey
        public String getKey() {
            return value;
        }
    }

    public NodeManager addOutputCatchStage() throws InterruptedException {
        // =======================================================
        // configure an output catcher tier
        final Node out = new Node.Builder("test-app").defaultRoutingStrategyId("net.dempsy.router.simple")
            .receiver(new BlockingQueueReceiver(new ArrayBlockingQueue<>(16))).nodeStatsCollector(new BasicNodeStatsCollector())
            .cluster("output-catch").mp(new MessageProcessor<OutputCatcher>(new OutputCatcher()))
            .build();
        out.validate();

        final NodeManager nman = track(new NodeManager()).node(out).collaborator(track(sessionFactory.createSession())).start();
        // wait until we can actually reach the output-catch cluster from the main node
        assertTrue(poll(o -> {
            try {
                return canReach(getRouter(manager), "output-catch",
                    new KeyExtractor().extract(new OutputMessage("foo", 1, 1)).iterator().next());
            } catch(final Exception e) {
                return false;
            }
        }));
        // =======================================================
        return nman;
    }

    private TestProcessor createAndGet(final String foo) throws Exception {
        cache = new HashMap<>();
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage(foo));

        assertTrue(poll(o -> container.getProcessorCount() > 0));
        Thread.sleep(100);
        assertEquals(1, container.getProcessorCount(), "did not create MP");

        assertTrue(poll(cache, c -> c.get(foo) != null));
        final TestProcessor mp = cache.get(foo);

        assertNotNull(mp, "MP not associated with expected key");
        assertEquals(1, mp.activationCount, "activation count, 1st message");
        assertEquals(1, mp.invocationCount, "invocation count, 1st message");

        return mp;
    }

    // ----------------------------------------------------------------------------
    // Message and MP classes
    // ----------------------------------------------------------------------------

    @Mp
    public static class TestProcessor implements Cloneable {
        public volatile String myKey;
        public volatile int activationCount;
        public volatile int invocationCount;
        public volatile int outputCount;
        public volatile AtomicBoolean evict = new AtomicBoolean(false);
        public AtomicInteger cloneCount = new AtomicInteger(0);
        public volatile CountDownLatch latch = new CountDownLatch(0);

        public static AtomicLong numOutputExecutions = new AtomicLong(0);
        public static CountDownLatch blockAllOutput = new CountDownLatch(0);
        public AtomicLong passivateCount = new AtomicLong(0);
        public CountDownLatch blockPassivate = new CountDownLatch(0);
        public AtomicBoolean throwPassivateException = new AtomicBoolean(false);
        public AtomicLong passivateExceptionCount = new AtomicLong(0);

        public AtomicLong startCalled = new AtomicLong(0);
        public ClusterId clusterId = null;

        @Start
        public void startMe(final ClusterId clusterId) {
            this.clusterId = clusterId;
            startCalled.incrementAndGet();
        }

        @Override
        public TestProcessor clone()
            throws CloneNotSupportedException {
            cloneCount.incrementAndGet();
            return (TestProcessor)super.clone();
        }

        @Activation
        public void activate(final byte[] data) {
            if(throwMeInActivation != null)
                throw throwMeInActivation;
            activationCount++;
        }

        @Passivation
        public void passivate() throws InterruptedException {
            passivateCount.incrementAndGet();

            blockPassivate.await();

            if(throwPassivateException.get()) {
                passivateExceptionCount.incrementAndGet();
                throw new RuntimeException("Passivate");
            }
        }

        @MessageHandler
        public void handle(final ContainerTestMessage message) throws InterruptedException {
            myKey = message.getKey();

            if(cache != null)
                cache.put(myKey, this);

            invocationCount++;

            latch.await();
        }

        @MessageHandler
        public ContainerTestMessage handle(final MyMessage message) throws InterruptedException {
            if(justThrowMe != null)
                throw justThrowMe;

            myKey = message.getKey();

            if(cache != null)
                cache.put(myKey, this);

            invocationCount++;

            latch.await();
            return new ContainerTestMessage(message.value);
        }

        @Evictable
        public boolean isEvictable() {
            return evict.get();
        }

        @Output
        public OutputMessage doOutput() throws InterruptedException {
            numOutputExecutions.incrementAndGet();
            try {
                blockAllOutput.await();
                return new OutputMessage(myKey, activationCount, invocationCount, outputCount++);
            } finally {
                numOutputExecutions.decrementAndGet();
            }
        }
    }

    @Mp
    public static class OutputCatcher implements Cloneable {
        @MessageHandler
        public ContainerTestMessage handle(final OutputMessage message) throws InterruptedException {
            if(outputMessages != null)
                outputMessages.add(message);
            return new ContainerTestMessage(message.mpKey);
        }

        @Override
        public OutputCatcher clone() throws CloneNotSupportedException {
            return (OutputCatcher)super.clone();
        }
    }

    public static class TestAdaptor implements Adaptor {
        public Dispatcher dispatcher;

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    // ----------------------------------------------------------------------------
    // Test Cases
    // ----------------------------------------------------------------------------
    public static final KeyExtractor ke = new KeyExtractor();

    public void doNothing() {}

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testWrongTypeMessage(final String containerId) throws Exception {
        setUp(containerId);
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(new KeyedMessage(kmwt.key, new Object()), Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testMpActivationFails(final String containerId) throws Exception {
        setUp(containerId);
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        throwMeInActivation = new RuntimeException("JustThrowMeDAMMIT!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());

    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testMpThrowsDempsyException(final String containerId) throws Exception {
        setUp(containerId);
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        justThrowMe = new DempsyException("JustThrowMe!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testMpThrowsException(final String containerId) throws Exception {
        setUp(containerId);
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        justThrowMe = new RuntimeException("JustThrowMe!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testConfiguration(final String containerId) throws Exception {
        setUp(containerId);
        // this assertion is superfluous, since we deref container in setUp()
        assertNotNull(container, "did not create container");
        assertEquals(new ClusterId("test-app", "test-cluster"), container.getClusterId());

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        assertEquals(1, prototype.startCalled.get());

        assertNotNull(prototype.clusterId);
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testFeedbackLoop(final String containerId) throws Exception {
        setUp(containerId);
        cache = new ConcurrentHashMap<>();
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);

        adaptor.dispatcher.dispatchAnnotated(new MyMessage("foo"));

        assertTrue(poll(o -> container.getProcessorCount() > 0));
        Thread.sleep(100);
        assertEquals(1, container.getProcessorCount(), "did not create MP");

        assertTrue(poll(cache, c -> c.get("foo") != null));
        final TestProcessor mp = cache.get("foo");
        assertNotNull(mp, "MP not associated with expected key");

        assertTrue(poll(mp, o -> o.invocationCount > 1));
        assertEquals(1, mp.activationCount, "activation count, 1st message");
        assertEquals(2, mp.invocationCount, "invocation count, 1st message");

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(mp, o -> o.invocationCount > 2));
        Thread.sleep(100);

        assertEquals(1, mp.activationCount, "activation count, 2nd message");
        assertEquals(3, mp.invocationCount, "invocation count, 2nd message");
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testMessageDispatch(final String containerId) throws Exception {
        setUp(containerId);
        cache = new ConcurrentHashMap<>();
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));

        assertTrue(poll(o -> container.getProcessorCount() > 0));
        Thread.sleep(100);
        assertEquals(1, container.getProcessorCount(), "did not create MP");

        assertTrue(poll(cache, c -> c.get("foo") != null));
        final TestProcessor mp = cache.get("foo");
        assertNotNull(mp, "MP not associated with expected key");
        assertEquals(1, mp.activationCount, "activation count, 1st message");
        assertEquals(1, mp.invocationCount, "invocation count, 1st message");

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(mp, o -> o.invocationCount > 1));
        Thread.sleep(100);

        assertEquals(1, mp.activationCount, "activation count, 2nd message");
        assertEquals(2, mp.invocationCount, "invocation count, 2nd message");
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testInvokeOutput(final String containerId) throws Exception {
        setUp(containerId);
        outputMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
        cache = new ConcurrentHashMap<>();

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("bar"));

        assertTrue(poll(container, c -> (c.getProcessorCount() + ((ClusterMetricGetters)c.statCollector).getMessageDiscardedCount()) > 1));
        Thread.sleep(100);

        assertEquals(2, container.getProcessorCount(), "number of MP instances");

        try(NodeManager nman = addOutputCatchStage();) {

            final TestProcessor mp = cache.get("foo");
            assertTrue(poll(mp, m -> mp.invocationCount > 0));
            Thread.sleep(100);
            assertEquals(1, mp.invocationCount, "invocation count, 1st message");

            // because the sessionFactory is shared and the appname is the same, we should be in the same app
            container.outputPass();

            assertTrue(poll(outputMessages, o -> o.size() > 1));
            Thread.sleep(100);
            assertEquals(2, outputMessages.size());

            // no new mps created in the first one
            assertEquals(2, container.getProcessorCount(), "did not create MP");

            // but the invocation count should have increased since the output cycles feeds messages back to this cluster
            assertTrue(poll(mp, m -> mp.invocationCount > 1));
            Thread.sleep(100);
            assertEquals(2, mp.invocationCount, "invocation count, 1st message");

            // // order of messages is not guaranteed, so we need to aggregate keys
            final HashSet<String> messageKeys = new HashSet<String>();

            final Iterator<OutputMessage> iter = outputMessages.iterator();
            messageKeys.add(iter.next().getKey());
            messageKeys.add(iter.next().getKey());
            assertTrue(messageKeys.contains("foo"), "first MP sent output");
            assertTrue(messageKeys.contains("bar"), "second MP sent output");
        }
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testMtInvokeOutput(final String containerId) throws Exception {
        setUp(containerId);
        outputMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final int numInstances = 20;

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);
        for(int i = 0; i < numInstances; i++) {
            adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo" + i));
            Thread.yield(); // help the container when it has a limited queue
        }

        assertTrue(poll(container, c -> (c.getProcessorCount() + ((ClusterMetricGetters)c.statCollector).getMessageDiscardedCount()) > 19));
        Thread.sleep(100);
        final long messagesDiscarded = ((ClusterMetricGetters)container.statCollector).getMessageDiscardedCount();
        assertEquals(20, container.getProcessorCount() + messagesDiscarded, "number of MP instances");

        try(NodeManager nman = addOutputCatchStage();) {
            container.outputPass();
            assertTrue(poll(outputMessages, o -> (o.size() + messagesDiscarded) > 19));
            Thread.sleep(100);
            assertEquals(20, outputMessages.size() + messagesDiscarded);
        }
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testEvictable(final String containerId) throws Exception {
        setUp(containerId);
        final TestProcessor mp = createAndGet("foo");

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        final int tmpCloneCount = prototype.cloneCount.intValue();

        mp.evict.set(true);
        container.evict();

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals(tmpCloneCount + 1, prototype.cloneCount.intValue(), "Clone count, 2nd message");
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testEvictableWithPassivateException(final String containerId) throws Exception {
        setUp(containerId);
        final TestProcessor mp = createAndGet("foo");
        mp.throwPassivateException.set(true);

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        final int tmpCloneCount = prototype.cloneCount.intValue();

        mp.evict.set(true);
        container.evict();
        assertTrue(poll(o -> mp.passivateExceptionCount.get() > 0));
        Thread.sleep(100);
        assertEquals(1, mp.passivateExceptionCount.get(), "Passivate Exception Thrown");

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals(tmpCloneCount + 1, prototype.cloneCount.intValue(), "Clone count, 2nd message");
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testEvictableWithBusyMp(final String containerId) throws Throwable {
        setUp(containerId);
        final TestProcessor mp = createAndGet("foo");

        // now we're going to cause the processing to be held up.
        mp.latch = new CountDownLatch(1);
        mp.evict.set(true); // allow eviction

        // sending it a message will now cause it to hang up while processing
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));

        final TestProcessor prototype = context.getBean(TestProcessor.class);

        // keep track of the cloneCount for later checking
        final int tmpCloneCount = prototype.cloneCount.intValue();

        // invocation count should go to 2
        assertTrue(poll(mp, o -> o.invocationCount == 2));

        // now kick off the evict in a separate thread since we expect it to hang
        // until the mp becomes unstuck.
        final AtomicBoolean evictIsComplete = new AtomicBoolean(false); // this will allow us to see the evict pass complete
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                container.evict();
                evictIsComplete.set(true);
            }
        });
        thread.start();

        // now check to make sure eviction doesn't complete.
        Thread.sleep(100); // just a little to give any mistakes a change to work themselves through
        assertFalse(evictIsComplete.get()); // make sure eviction didn't finish

        mp.latch.countDown(); // this lets it go

        // wait until the eviction completes
        assertTrue(poll(evictIsComplete, o -> o.get()));
        Thread.sleep(100);
        assertEquals(1, mp.activationCount, "activation count, 2nd message");
        assertEquals(2, mp.invocationCount, "invocation count, 2nd message");

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals(tmpCloneCount + 1, prototype.cloneCount.intValue(), "Clone count, 2nd message");
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testEvictCollisionWithBlocking(final String containerId) throws Throwable {
        setUp(containerId);
//        if(!container.containerIsThreadSafe())
//            return; // we can't run this test unless the container is thread safe.
        boolean countedDown = false;
        final TestProcessor mp = createAndGet("foo");
        try {

            // now we're going to cause the passivate to be held up.
            mp.blockPassivate = new CountDownLatch(1);
            mp.evict.set(true); // allow eviction

            // now kick off the evict in a separate thread since we expect it to hang
            // until the mp becomes unstuck.
            final AtomicBoolean evictIsComplete = new AtomicBoolean(false); // this will allow us to see the evict pass complete
            final Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    container.evict();
                    evictIsComplete.set(true);
                }
            });
            thread.start();

            Thread.sleep(500); // let it get going.
            // assertFalse(evictIsComplete.get()); // check to see we're hung.

            final ClusterMetricGetters sc = (ClusterMetricGetters)statsCollector;
            assertEquals(0, sc.getMessageCollisionCount());

            // sending it a message will now cause it to have the collision tick up
            final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
            adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));

            // give it some time.
            Thread.sleep(100);

            // make sure there's no collision
            assertEquals(0, sc.getMessageCollisionCount());

            // make sure no message got handled
            assertEquals(1, mp.invocationCount); // 1 is the initial invocation that caused the instantiation.

            // now let the evict finish
            countedDown = true;
            mp.blockPassivate.countDown();

            // wait until the eviction completes
            assertTrue(poll(evictIsComplete, o -> o.get()));

            // Once the poll finishes a new Mp is instantiated and handling messages.
            assertTrue(poll(cache, c -> c.get("foo") != null));
            final TestProcessor mp2 = cache.get("foo");
            assertNotNull(mp, "MP not associated with expected key");

            // invocationCount should be 1 from the initial invocation that caused the clone, and no more
            assertEquals(1, mp.invocationCount);
            assertEquals(1, mp2.invocationCount);
            assertTrue(mp != mp2);

            // send a message that should go through
            adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
            assertTrue(poll(o -> mp2.invocationCount > 1));
            Thread.sleep(100);
            assertEquals(1, mp.invocationCount);
            assertEquals(2, mp2.invocationCount);
        } finally {
            if(!countedDown && mp.blockPassivate != null) {
                mp.blockPassivate.countDown();
            }
        }
    }
}
