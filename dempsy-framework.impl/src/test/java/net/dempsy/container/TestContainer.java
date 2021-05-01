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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
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
import net.dempsy.transport.blockingqueue.BlockingQueueReceiver;
import net.dempsy.util.SystemPropertyManager;

//
// NOTE: this test simply puts messages on an input queue, and expects
// messages on an output queue; the important part is the wiring
// in TestMPContainer.xml
//
@RunWith(Parameterized.class)
public class TestContainer {
    public static String[] ctx = {
        "classpath:/spring/container/test-container.xml",
        "classpath:/spring/container/test-mp.xml",
        "classpath:/spring/container/test-adaptor.xml"
    };

    @Parameters(name = "{index}: container type={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {LockingContainer.class.getPackage().getName()},
            // the NonLockingContainer is broken
            // {NonLockingContainer.class.getPackage().getName()},
            {NonLockingAltContainer.class.getPackage().getName()},
        });
    }

    private Container container = null;

    private ClassPathXmlApplicationContext context = null;
    private NodeManager manager;
    private LocalClusterSessionFactory sessionFactory = null;
    private ClusterStatsCollector statsCollector;

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private final String containerId;

    public static Map<String, TestProcessor> cache = null;
    public static Set<OutputMessage> outputMessages = null;
    public static RuntimeException justThrowMe = null;
    public static RuntimeException throwMeInActivation = null;

    public TestContainer(final String containerId) {
        this.containerId = containerId;
    }

    private <T extends AutoCloseable> T track(final T o) {
        toClose.add(o);
        return o;
    }

    @Before
    public void setUp() throws Exception {
        justThrowMe = null;
        throwMeInActivation = null;
        track(new SystemPropertyManager()).set("container-type", containerId);
        context = track(new ClassPathXmlApplicationContext(ctx));
        sessionFactory = new LocalClusterSessionFactory();
        final Node node = context.getBean(Node.class);
        manager = track(new NodeManager()).node(node).collaborator(track(sessionFactory.createSession())).start();
        statsCollector = manager.getClusterStatsCollector(new ClusterId("test-app", "test-cluster"));
        container = manager.getContainers().get(0);
        assertTrue(poll(manager, m -> m.isReady()));
    }

    @After
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
        assertEquals("did not create MP", 1, container.getProcessorCount());

        assertTrue(poll(cache, c -> c.get(foo) != null));
        final TestProcessor mp = cache.get(foo);

        assertNotNull("MP not associated with expected key", mp);
        assertEquals("activation count, 1st message", 1, mp.activationCount);
        assertEquals("invocation count, 1st message", 1, mp.invocationCount);

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

    @Test
    public void testWrongTypeMessage() throws Exception {
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(new KeyedMessage(kmwt.key, new Object()), Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
    }

    @Test
    public void testMpActivationFails() throws Exception {
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        throwMeInActivation = new RuntimeException("JustThrowMeDAMMIT!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());

    }

    @Test
    public void testMpThrowsDempsyException() throws Exception {
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        justThrowMe = new DempsyException("JustThrowMe!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
    }

    @Test
    public void testMpThrowsException() throws Exception {
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(0, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
        justThrowMe = new RuntimeException("JustThrowMe!");
        final KeyedMessageWithType kmwt = ke.extract(new MyMessage("YO")).get(0);
        container.dispatch(kmwt, Operation.handle, true);
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getMessageFailedCount());
        assertEquals(1, ((ClusterMetricGetters)container.statCollector).getDispatchedMessageCount());
    }

    @Test
    public void testConfiguration() throws Exception {
        // this assertion is superfluous, since we deref container in setUp()
        assertNotNull("did not create container", container);
        assertEquals(new ClusterId("test-app", "test-cluster"), container.getClusterId());

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        assertEquals(1, prototype.startCalled.get());

        assertNotNull(prototype.clusterId);
    }

    @Test
    public void testFeedbackLoop() throws Exception {
        cache = new ConcurrentHashMap<>();
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);

        adaptor.dispatcher.dispatchAnnotated(new MyMessage("foo"));

        assertTrue(poll(o -> container.getProcessorCount() > 0));
        Thread.sleep(100);
        assertEquals("did not create MP", 1, container.getProcessorCount());

        assertTrue(poll(cache, c -> c.get("foo") != null));
        final TestProcessor mp = cache.get("foo");
        assertNotNull("MP not associated with expected key", mp);

        assertTrue(poll(mp, o -> o.invocationCount > 1));
        assertEquals("activation count, 1st message", 1, mp.activationCount);
        assertEquals("invocation count, 1st message", 2, mp.invocationCount);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(mp, o -> o.invocationCount > 2));
        Thread.sleep(100);

        assertEquals("activation count, 2nd message", 1, mp.activationCount);
        assertEquals("invocation count, 2nd message", 3, mp.invocationCount);
    }

    @Test
    public void testMessageDispatch() throws Exception {
        cache = new ConcurrentHashMap<>();
        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));

        assertTrue(poll(o -> container.getProcessorCount() > 0));
        Thread.sleep(100);
        assertEquals("did not create MP", 1, container.getProcessorCount());

        assertTrue(poll(cache, c -> c.get("foo") != null));
        final TestProcessor mp = cache.get("foo");
        assertNotNull("MP not associated with expected key", mp);
        assertEquals("activation count, 1st message", 1, mp.activationCount);
        assertEquals("invocation count, 1st message", 1, mp.invocationCount);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(mp, o -> o.invocationCount > 1));
        Thread.sleep(100);

        assertEquals("activation count, 2nd message", 1, mp.activationCount);
        assertEquals("invocation count, 2nd message", 2, mp.invocationCount);
    }

    @Test
    public void testInvokeOutput() throws Exception {
        outputMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
        cache = new ConcurrentHashMap<>();

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        assertNotNull(adaptor.dispatcher);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("bar"));

        assertTrue(poll(container, c -> (c.getProcessorCount() + ((ClusterMetricGetters)c.statCollector).getMessageDiscardedCount()) > 1));
        Thread.sleep(100);

        assertEquals("number of MP instances", 2, container.getProcessorCount());

        try(NodeManager nman = addOutputCatchStage();) {

            final TestProcessor mp = cache.get("foo");
            assertTrue(poll(mp, m -> mp.invocationCount > 0));
            Thread.sleep(100);
            assertEquals("invocation count, 1st message", 1, mp.invocationCount);

            // because the sessionFactory is shared and the appname is the same, we should be in the same app
            container.outputPass();

            assertTrue(poll(outputMessages, o -> o.size() > 1));
            Thread.sleep(100);
            assertEquals(2, outputMessages.size());

            // no new mps created in the first one
            assertEquals("did not create MP", 2, container.getProcessorCount());

            // but the invocation count should have increased since the output cycles feeds messages back to this cluster
            assertTrue(poll(mp, m -> mp.invocationCount > 1));
            Thread.sleep(100);
            assertEquals("invocation count, 1st message", 2, mp.invocationCount);

            // // order of messages is not guaranteed, so we need to aggregate keys
            final HashSet<String> messageKeys = new HashSet<String>();

            final Iterator<OutputMessage> iter = outputMessages.iterator();
            messageKeys.add(iter.next().getKey());
            messageKeys.add(iter.next().getKey());
            assertTrue("first MP sent output", messageKeys.contains("foo"));
            assertTrue("second MP sent output", messageKeys.contains("bar"));
        }
    }

    @Test
    public void testMtInvokeOutput() throws Exception {
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
        assertEquals("number of MP instances", 20, container.getProcessorCount() + messagesDiscarded);

        try(NodeManager nman = addOutputCatchStage();) {
            container.outputPass();
            assertTrue(poll(outputMessages, o -> (o.size() + messagesDiscarded) > 19));
            Thread.sleep(100);
            assertEquals(20, outputMessages.size() + messagesDiscarded);
        }
    }

    @Test
    public void testEvictable() throws Exception {
        final TestProcessor mp = createAndGet("foo");

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        final int tmpCloneCount = prototype.cloneCount.intValue();

        mp.evict.set(true);
        container.evict();

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals("Clone count, 2nd message", tmpCloneCount + 1, prototype.cloneCount.intValue());
    }

    @Test
    public void testEvictableWithPassivateException() throws Exception {
        final TestProcessor mp = createAndGet("foo");
        mp.throwPassivateException.set(true);

        final TestProcessor prototype = context.getBean(TestProcessor.class);
        final int tmpCloneCount = prototype.cloneCount.intValue();

        mp.evict.set(true);
        container.evict();
        assertTrue(poll(o -> mp.passivateExceptionCount.get() > 0));
        Thread.sleep(100);
        assertEquals("Passivate Exception Thrown", 1, mp.passivateExceptionCount.get());

        final TestAdaptor adaptor = context.getBean(TestAdaptor.class);
        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals("Clone count, 2nd message", tmpCloneCount + 1, prototype.cloneCount.intValue());
    }

    @Test
    public void testEvictableWithBusyMp() throws Throwable {
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
        assertEquals("activation count, 2nd message", 1, mp.activationCount);
        assertEquals("invocation count, 2nd message", 2, mp.invocationCount);

        adaptor.dispatcher.dispatchAnnotated(new ContainerTestMessage("foo"));
        assertTrue(poll(o -> prototype.cloneCount.intValue() > tmpCloneCount));
        Thread.sleep(1000);
        assertEquals("Clone count, 2nd message", tmpCloneCount + 1, prototype.cloneCount.intValue());
    }

    @Test
    public void testEvictCollisionWithBlocking() throws Throwable {
        final TestProcessor mp = createAndGet("foo");

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
        assertFalse(evictIsComplete.get()); // check to see we're hung.

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
        mp.blockPassivate.countDown();

        // wait until the eviction completes
        assertTrue(poll(evictIsComplete, o -> o.get()));

        // Once the poll finishes a new Mp is instantiated and handling messages.
        assertTrue(poll(cache, c -> c.get("foo") != null));
        final TestProcessor mp2 = cache.get("foo");
        assertNotNull("MP not associated with expected key", mp);

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
    }
}
