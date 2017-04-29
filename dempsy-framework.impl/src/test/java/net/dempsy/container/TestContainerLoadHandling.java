/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.container;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Manager;
import net.dempsy.ServiceTracker;
import net.dempsy.config.ClusterId;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.container.locking.LockingContainer;
import net.dempsy.container.mocks.MockInputMessage;
import net.dempsy.container.mocks.MockOutputMessage;
import net.dempsy.container.nonlocking.NonLockingContainer;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.monitoring.basic.BasicClusterStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.util.TestInfrastructure;

/**
 * Test load handling / shedding in the MP container. This is probably involved enough to merit
 *  not mixing into the existing MPContainer test cases.
 */
@RunWith(Parameterized.class)
public class TestContainerLoadHandling {
    private void checkStat(final ClusterMetricGetters stat) {
        assertEquals(stat.getDispatchedMessageCount(),
                stat.getMessageFailedCount() + stat.getProcessedMessageCount() + stat.getInFlightMessageCount());
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { LockingContainer.class.getPackage().getName() },
                { NonLockingContainer.class.getPackage().getName() },
                { NonLockingAltContainer.class.getPackage().getName() },
        });
    }

    private final String containerId;

    public TestContainerLoadHandling(final String containerId) {
        this.containerId = containerId;
    }

    private static int NUMMPS = 10;
    private static int NUMTHREADS = 8;
    private static int MSGPERTHREAD = 10;
    private static int NUMRUNS = 50;
    private static int DUPFACTOR = 10;

    private static Logger logger = LoggerFactory.getLogger(TestContainerLoadHandling.class);

    private Container container;
    private BasicClusterStatsCollector clusterStats;
    private BasicNodeStatsCollector nodeStats;
    private MockDispatcher dispatcher;
    private volatile boolean forceOutputException = false;

    private int sequence = 0;

    ServiceTracker tr = new ServiceTracker();

    @Before
    public void setUp() throws Exception {
        final ClusterId cid = new ClusterId("TestContainerLoadHandling", "test" + sequence++);
        dispatcher = new MockDispatcher();

        final BasicClusterStatsCollector sc = new BasicClusterStatsCollector();
        clusterStats = sc;
        nodeStats = new BasicNodeStatsCollector();

        container = tr.track(new Manager<Container>(Container.class).getAssociatedInstance(containerId))
                .setMessageProcessor(new MessageProcessor<TestMessageProcessor>(new TestMessageProcessor()))
                .setClusterId(cid);
        container.setDispatcher(dispatcher);

        container.start(new TestInfrastructure(null, null) {

            @Override
            public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
                return sc;
            }

            @Override
            public NodeStatsCollector getNodeStatsCollector() {
                return nodeStats;
            }
        });

        forceOutputException = false;
        stillRunning = true;
    }

    @After
    public void tearDown() throws Exception {
        tr.stopAll();
        ((StatsCollector) clusterStats).stop();
    }

    public static boolean failed = false;
    public static boolean stillRunning = true;

    public static class SendMessageThread implements Runnable {
        final Container mpc;
        final KeyedMessage[] messages;
        final boolean block;
        final Random random = new Random();
        final int numMsg;

        static AtomicLong finishedCount = new AtomicLong(0);
        static CountDownLatch startLatch = new CountDownLatch(0);
        static AtomicLong processingCount = new AtomicLong(0);

        public SendMessageThread(final Container mpc, final KeyedMessage[] messages, final boolean block, final int numMsg) {
            this.mpc = mpc;
            this.messages = messages;
            this.block = block;
            this.numMsg = numMsg;
        }

        public SendMessageThread(final Container mpc, final KeyedMessage[] messages, final boolean block) {
            this(mpc, messages, block, -1);
        }

        @Override
        public void run() {
            try {
                processingCount.incrementAndGet();
                startLatch.await(); // wait for the command to go
                if (numMsg < 0) {
                    while (stillRunning) {
                        final KeyedMessage message = messages[random.nextInt(messages.length)];
                        mpc.dispatch(message, block);
                    }
                } else {
                    for (int i = 0; i < numMsg; i++) {
                        final KeyedMessage message = messages[random.nextInt(messages.length)];
                        mpc.dispatch(message, block);
                    }
                }
            } catch (final Exception e) {
                failed = true;
                System.out.println("FAILED!");
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                finishedCount.incrementAndGet();
                processingCount.decrementAndGet();
            }
        }
    }

    private static KeyExtractor extractor = new KeyExtractor();

    private static KeyedMessageWithType km(final Object message) throws Exception {
        return extractor.extract(message).get(0);
    }

    /*
     * Utility methods for tests
     */
    public static Thread startMessagePump(final Container container, final KeyedMessage[] messages, final boolean block) {
        return chain(new Thread(new SendMessageThread(container, messages, block)), t -> t.start());
    }

    public static Thread sendMessages(final Container container, final KeyedMessage[] messages, final boolean block, final int count) {
        return chain(new Thread(new SendMessageThread(container, messages, block, count)), t -> t.start());
    }

    /*
     * Test Infastructure
     */
    class MockDispatcher extends Dispatcher {
        public List<KeyedMessageWithType> messages = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void dispatch(final KeyedMessageWithType message) {
            assertNotNull(message);
            messages.add(message);
        }

    }

    CountDownLatch commonLongRunningHandler = null;

    @Mp
    public class TestMessageProcessor implements Cloneable {
        int messageCount = 0;
        String key;

        @Override
        public TestMessageProcessor clone() throws CloneNotSupportedException {
            final TestMessageProcessor ret = (TestMessageProcessor) super.clone();
            return ret;
        }

        @MessageHandler
        public MockOutputMessage handleMessage(final MockInputMessage msg) throws InterruptedException {
            logger.trace("handling key " + msg.getKey() + " count is " + messageCount);
            if (commonLongRunningHandler != null)
                commonLongRunningHandler.await();
            key = msg.getKey();
            messageCount++;
            msg.setProcessed(true);
            final MockOutputMessage out = new MockOutputMessage(msg.getKey());
            return out;
        }

        @Output
        public MockOutputMessage doOutput() throws InterruptedException {
            logger.trace("handling output message for mp with key " + key);
            final MockOutputMessage out = new MockOutputMessage(key, "output");

            if (forceOutputException)
                throw new RuntimeException("Forced Exception!");

            return out;
        }
    }

    /*
     * Actual Unit Tests
     */

    public void runSimpleProcessingWithoutQueuing(final boolean block) throws Exception {
        SendMessageThread.startLatch = new CountDownLatch(1); // set up a gate for starting

        // produce the initial MPs
        final ArrayList<Thread> in = new ArrayList<Thread>();
        final KeyedMessage[] messages = new KeyedMessage[NUMMPS];
        for (int i = 0; i < NUMMPS; i++)
            messages[i] = km(new MockInputMessage("key" + i));

        for (int j = 0; j < NUMMPS; j++)
            in.add(startMessagePump(container, messages, block));

        SendMessageThread.startLatch.countDown(); // let the messages go.

        assertTrue(poll(o -> container.getProcessorCount() == NUMMPS));
        stillRunning = false;
        assertTrue(poll(o -> in.stream().filter(t -> t.isAlive()).count() == 0));

        final int numMessagePumped = dispatcher.messages.size();
        final long initialDiscard = clusterStats.getMessageCollisionCount();
        dispatcher.messages.clear();

        // Send NTHREADS Messages and wait for them to come out, then repeat
        for (int i = 0; i < NUMRUNS; i++) {
            SendMessageThread.startLatch = new CountDownLatch(1); // set up a gate for starting
            SendMessageThread.finishedCount.set(0);
            SendMessageThread.processingCount.set(0);

            in.clear();
            for (int j = 0; j < NUMTHREADS; j++)
                in.add(sendMessages(container, messages, block, MSGPERTHREAD));

            assertTrue(poll(o -> SendMessageThread.processingCount.get() == NUMTHREADS));
            SendMessageThread.startLatch.countDown(); // let the messages go.

            // after sends are allowed to proceed
            assertTrue("Timeout waiting on message to be sent", poll(o -> SendMessageThread.finishedCount.get() >= NUMTHREADS));
            assertEquals(NUMTHREADS, SendMessageThread.finishedCount.get());

            assertTrue(poll(o -> clusterStats.getInFlightMessageCount() == 0));
            assertTrue(poll(o -> in.stream().filter(t -> t.isAlive() == true).count() == 0));

            final long discarded = clusterStats.getMessageCollisionCount();
            if (block)
                assertEquals(0L, discarded);

            final int iter = i;
            assertEquals((NUMTHREADS * MSGPERTHREAD) * (iter + 1), dispatcher.messages.size() + discarded - initialDiscard);
            assertEquals((NUMTHREADS * MSGPERTHREAD) * (iter + 1) + numMessagePumped,
                    clusterStats.getProcessedMessageCount() + discarded - initialDiscard);
        }

        checkStat(clusterStats);
    }

    /**
     * Test the simple case of messages arriving slowly and always having a thread available.
     */
    @Test
    public void testSimpleProcessingWithoutQueuingBlocking() throws Exception {
        runSimpleProcessingWithoutQueuing(true);
    }

    /**
     * Test the simple case of messages arriving slowly and always having a thread available.
     */
    @Test
    public void testSimpleProcessingWithoutQueuingNonBlocking() throws Exception {
        runSimpleProcessingWithoutQueuing(false);
    }

    /**
     * Test the case where messages arrive faster than they are processed but the queue never exceeds the Thread Pool Size * 2, so no messages are discarded.
     */
    @Test
    public void testMessagesCanQueueWithinLimitsBlocking() throws Exception {
        // produce the initial MPs
        final ArrayList<Thread> in = new ArrayList<Thread>();
        final KeyedMessage[] messages = new KeyedMessage[NUMMPS];
        for (int i = 0; i < NUMMPS; i++)
            messages[i] = km(new MockInputMessage("key" + i));

        for (int j = 0; j < NUMMPS; j++)
            in.add(sendMessages(container, new KeyedMessage[] { messages[j] }, true, 1));

        SendMessageThread.startLatch.countDown(); // let the messages go.
        assertTrue(poll(o -> in.stream().filter(t -> t.isAlive()).count() == 0));

        assertEquals(NUMMPS, container.getProcessorCount());

        final int numMessagePumped = dispatcher.messages.size();
        assertEquals(numMessagePumped, clusterStats.getProcessedMessageCount());
        dispatcher.messages.clear();

        for (int i = 0; i < NUMRUNS; i++) {
            final int iter = i;
            final long initialMessagesDispatched = clusterStats.getDispatchedMessageCount();

            SendMessageThread.startLatch = new CountDownLatch(1); // set up a gate for starting
            SendMessageThread.finishedCount.set(0);
            SendMessageThread.processingCount.set(0);

            commonLongRunningHandler = new CountDownLatch(1);

            in.clear();
            for (int j = 0; j < NUMMPS; j++)
                in.add(sendMessages(container, new KeyedMessage[] { km(new MockInputMessage("key" + j % NUMMPS)) }, true, DUPFACTOR));

            assertTrue(poll(o -> SendMessageThread.processingCount.get() == NUMMPS));
            SendMessageThread.startLatch.countDown(); // let the messages go.

            // NUMMPS messages should be "dispatched"
            assertTrue(poll(o -> (clusterStats.getDispatchedMessageCount() - initialMessagesDispatched) == NUMMPS));

            // 1 message from each thread should be "in flight"
            assertTrue(poll(o -> container.getMessageWorkingCount() == NUMMPS));

            Thread.sleep(50);

            // NUMMPS messages should STILL be "dispatched"
            assertTrue(poll(o -> (clusterStats.getDispatchedMessageCount() - initialMessagesDispatched) == NUMMPS));

            commonLongRunningHandler.countDown(); // let the rest of them go

            assertTrue("Timeout waiting on message to be sent", poll(o -> in.stream().filter(t -> t.isAlive() == true).count() == 0));

            // after sends are allowed to proceed
            assertEquals(NUMMPS, SendMessageThread.finishedCount.get());

            assertEquals(0, clusterStats.getInFlightMessageCount());

            assertEquals((DUPFACTOR * NUMMPS) * (iter + 1), dispatcher.messages.size());
            assertEquals((DUPFACTOR * NUMMPS) * (iter + 1) + numMessagePumped, clusterStats.getProcessedMessageCount());
            assertEquals(0L, clusterStats.getMessageCollisionCount());
            checkStat(clusterStats);
        }
    }

    // @Test
    // public void testExcessLoadIsDiscarded() throws Exception {
    // SendMessageThread.startLatch = new CountDownLatch(1); // set up a gate for starting
    // SendMessageThread.finishedCount.set(0);
    // SendMessageThread.processingCount.set(0);
    //
    // commonLongRunningHandler = new CountDownLatch(1);
    //
    // final ArrayList<Thread> in = new ArrayList<Thread>();
    // for (int j = 0; j < (DUPFACTOR * NUMMPS); j++)
    // in.add(sendMessage(container, new MockInputMessage("key" + j % NUMMPS), false));
    //
    // assertTrue(poll(o -> SendMessageThread.processingCount.get() == DUPFACTOR * NUMMPS));
    // SendMessageThread.startLatch.countDown(); // let the messages go.
    //
    // // Add another message. Since processing is waiting on the start latch,
    // /// it should be discarded
    // assertTrue(imIn.await(2, TimeUnit.SECONDS)); // this means they're all in
    // // need to directly dispatch it to avoid a race condition
    // container.dispatch(km(new MockInputMessage("key" + 0)), false);
    // assertEquals(1, stats.getDiscardedMessageCount());
    // container.dispatch(km(new MockInputMessage("key" + 1)), false);
    // assertEquals(2, stats.getDiscardedMessageCount());
    //
    // checkStat(stats);
    // startLatch.countDown();
    // checkStat(stats);
    //
    // // after sends are allowed to proceed
    // assertTrue("Timeout waiting on message to be sent", SendMessageThread.latch.await(2, TimeUnit.SECONDS));
    //
    // assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
    // while (stats.getInFlightMessageCount() > 0) {
    // Thread.yield();
    // }
    //
    // assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
    // assertEquals(3 * NUMMPS, dispatcher.messages.size());
    // assertEquals(3 * NUMMPS, stats.getProcessedMessageCount());
    // dispatcher.latch = new CountDownLatch(3 * NUMMPS); // reset the latch
    //
    // // Since the queue is drained, we should be able to drop a few more
    // // on and they should just go
    // finishLatch = new CountDownLatch(3 * NUMMPS);
    // SendMessageThread.latch = new CountDownLatch(in.size());
    // for (final MockInputMessage m : in) {
    // sendMessage(container, m, false);
    // Thread.sleep(50);
    // assertEquals(2, stats.getDiscardedMessageCount());
    // }
    // assertTrue("Timeout waiting on message to be sent", SendMessageThread.latch.await(2, TimeUnit.SECONDS));
    //
    // assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
    // while (stats.getInFlightMessageCount() > 0)
    // Thread.yield();
    //
    // assertTrue("Timeout waiting for MPs", dispatcher.latch.await(2, TimeUnit.SECONDS));
    // assertEquals(2 * 3 * NUMMPS, dispatcher.messages.size());
    // assertEquals(2 * 3 * NUMMPS, stats.getProcessedMessageCount());
    // checkStat(stats);
    // }

    // @Test
    // public void testOutputOperationsNotDiscarded() throws Exception {
    //
    // // prime the container with MP instances, by processing 4 * NTHREADS messages
    // startLatch = new CountDownLatch(0); // do not hold up the starting of processing
    // finishLatch = new CountDownLatch(NUMMPS * 4); // we expect this many messages to be handled.
    // imIn = new CountDownLatch(4 * NUMMPS); // this is how many times the message processor handler has been entered
    // dispatcher.latch = new CountDownLatch(4 * NUMMPS); // this counts down whenever a message is dispatched
    // SendMessageThread.latch = new CountDownLatch(4 * NUMMPS); // when spawning a thread to send a message into an Container, this counts down once the message sending is complete
    //
    // // invoke NTHREADS * 4 discrete messages each of which should cause an Mp to be created
    // for (int i = 0; i < (NUMMPS * 4); i++)
    // sendMessage(container, new MockInputMessage("key" + i), false);
    //
    // // wait for all of the messages to have been sent.
    // assertTrue("Timeout waiting on message to be sent", SendMessageThread.latch.await(2, TimeUnit.SECONDS));
    //
    // // wait for all of the messages to have been handled by the new Mps
    // assertTrue("Timeout on initial messages", finishLatch.await(4, TimeUnit.SECONDS));
    //
    // // the above can be triggered while still within the message processor handler so we wait
    // // for the stats collector to have registered the processed messages.
    // assertTrue(poll(o -> stats.getInFlightMessageCount() == 0));
    //
    // // with the startLatch set to 0 these should all complete.
    // assertEquals(4 * NUMMPS, stats.getProcessedMessageCount());
    //
    // // now we are going to hold the Mps inside the message handler
    // startLatch = new CountDownLatch(1);
    // // we are going to send 2 messages (per thread)
    // finishLatch = new CountDownLatch(2 * NUMMPS);
    // // ... and wait for the 2 message (per thread) to be in process (this startLatch will hold these from exiting)
    // imIn = new CountDownLatch(2 * NUMMPS); // (2 messages + 4 output) * nthreads
    //
    // // send 2 message (per thread) which will be held up by the startLatch
    // for (int i = 0; i < (2 * NUMMPS); i++) {
    // sendMessage(container, new MockInputMessage("key" + i), false);
    // assertEquals(0, stats.getDiscardedMessageCount());
    // }
    //
    // // there ought to be 2 * NTHREADS inflight ops since the startLatch isn't triggered
    // assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS));
    // checkStat(stats);
    // assertEquals(2 * NUMMPS, stats.getInFlightMessageCount());
    //
    // // with those held up in message handling they wont have the output invoked. So when we invoke output we need to count
    // // how many get through while the message processors are being held up. That means there should be 2 * NTHREAD held up
    // // in message handling and 2 * NTHREAD more that can execute the output (since the total is 4 * NTHREAD).
    // finishOutputLatch = new CountDownLatch(2 * NUMMPS); // we should see 2 output
    //
    // final long totalProcessedCount = stats.getProcessedMessageCount();
    // // Generate an output message
    // imIn = new CountDownLatch(2 * NUMMPS);
    //
    // // kick off the output pass in another thread
    // new Thread(new Runnable() {
    // @Override
    // public void run() {
    // container.outputPass();
    // }
    // }).start();
    //
    // // 2 * NTHREADS are at startLatch while there are 4 * NTHREADS total MPs.
    // // Thererfore two outputs should have executed and two more are thrashing now.
    // assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS)); // wait for the 2 * NTHREADS output calls to be entered
    // assertTrue("Timeout on initial messages", finishOutputLatch.await(4 * NUMMPS, TimeUnit.SECONDS)); // wait for those two to actually finish output processing
    //
    // // there's a race condition between finishing the output and the output call being registered so we need to wait
    // for (final long endTime = System.currentTimeMillis() + (NUMMPS * 4000); endTime > System.currentTimeMillis() &&
    // ((2L * NUMMPS) + totalProcessedCount != stats.getProcessedMessageCount());)
    // Thread.sleep(1);
    //
    // assertEquals((2L * NUMMPS) + totalProcessedCount, stats.getProcessedMessageCount());
    //
    // assertEquals(0, stats.getDiscardedMessageCount()); // no discarded messages
    // checkStat(stats);
    //
    // imIn = new CountDownLatch(2 * NUMMPS); // give me a place to wait for the remaining outputs.
    // startLatch.countDown(); // let the 2 MPs that are waiting run.
    // assertTrue("Timeout waiting for MPs", finishLatch.await(6, TimeUnit.SECONDS));
    // assertTrue("Timeout waiting for MP outputs to finish", imIn.await(6, TimeUnit.SECONDS));
    //
    // while (stats.getInFlightMessageCount() > 0)
    // Thread.yield();
    //
    // assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
    //
    // // all output messages were processed
    // int outMessages = 0;
    // for (final KeyedMessageWithType o : dispatcher.messages) {
    // final MockOutputMessage m = (MockOutputMessage) o.message;
    // if (m == null)
    // fail("wtf!?");
    // if ("output".equals(m.getType()))
    // outMessages++;
    // }
    // assertEquals(4 * NUMMPS, outMessages);
    // checkStat(stats);
    // }
    //
    // @Test
    // public void testOutputOperationsAloneDoNotCauseDiscard() throws Exception {
    // // prime the container with MP instances, by processing 4 * NTHREADS messages
    // startLatch = new CountDownLatch(0);
    // finishLatch = new CountDownLatch(NUMMPS * 4);
    // imIn = new CountDownLatch(0);
    // dispatcher.latch = new CountDownLatch(10 * NUMMPS);
    //
    // SendMessageThread.latch = new CountDownLatch(NUMMPS * 4);
    // for (int i = 0; i < (NUMMPS * 4); i++)
    // sendMessage(container, new MockInputMessage("key" + i), false);
    // assertTrue("Timeout on initial messages", finishLatch.await(4, TimeUnit.SECONDS));
    // Thread.yield(); // cover any deltas between mp decrementing latch and returning
    // assertTrue("Timeout waiting on message to be sent", SendMessageThread.latch.await(2, TimeUnit.SECONDS));
    // checkStat(stats);
    //
    // assertEquals(NUMMPS * 4, stats.getProcessedMessageCount());
    //
    // startLatch = new CountDownLatch(1);
    // finishLatch = new CountDownLatch(6 * NUMMPS); // (4 output + 2 msg) * nthreads
    // // Generate an output message but force a failure.
    // forceOutputException = true;
    // container.outputPass(); // this should be ok inline.
    // forceOutputException = false;
    // assertEquals(0, stats.getDiscardedMessageCount()); // no discarded messages
    // assertEquals(NUMMPS * 4, stats.getMessageFailedCount()); // all output messages should have failed.
    //
    // // run it again without the null pointer exception
    // finishOutputLatch = new CountDownLatch(NUMMPS * 4);
    // container.outputPass(); // this should be ok inline.
    // assertEquals(0, stats.getDiscardedMessageCount()); // no discarded messages
    // assertEquals(NUMMPS * 4, stats.getMessageFailedCount()); // all output messages should have failed.
    // assertEquals(2 * (NUMMPS * 4), stats.getProcessedMessageCount());
    // checkStat(stats);
    //
    // imIn = new CountDownLatch(2 * NUMMPS);
    // finishLatch = new CountDownLatch(2 * NUMMPS);
    // SendMessageThread.latch = new CountDownLatch(NUMMPS * 2);
    // // put 2 * nthreads messages on the queue, expect no discards
    // for (int i = 0; i < (2 * NUMMPS); i++) {
    // sendMessage(container, new MockInputMessage("key" + i), false);
    // assertEquals(0, stats.getDiscardedMessageCount());
    // }
    //
    // assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS));
    // assertEquals(0, stats.getDiscardedMessageCount());
    // checkStat(stats);
    //
    // // let things run
    // startLatch.countDown();
    // assertTrue("Timeout waiting on MPs", finishLatch.await(6, TimeUnit.SECONDS));
    //
    // // after sends are allowed to proceed
    // assertTrue("Timeout waiting on message to be sent", SendMessageThread.latch.await(2, TimeUnit.SECONDS));
    //
    // assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
    // assertEquals(0, stats.getDiscardedMessageCount());
    // assertEquals(10 * NUMMPS, dispatcher.messages.size());
    // }
}
