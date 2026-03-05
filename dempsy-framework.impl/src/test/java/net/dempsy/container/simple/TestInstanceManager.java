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

package net.dempsy.container.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.container.Container;
import net.dempsy.container.Container.Operation;
import net.dempsy.container.ContainerException;
import net.dempsy.container.mocks.DummyInbound;
import net.dempsy.container.simple.SimpleContainer.InstanceWrapper;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.messages.MessageResourceManager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.basic.BasicClusterStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.util.TestInfrastructure;

public class TestInstanceManager {

    private SimpleContainer container;

    // ----------------------------------------------------------------------------
    // Test classes -- must be static/public for introspection
    // ----------------------------------------------------------------------------

    @MessageType
    public static class MessageOne {
        private final Integer keyValue;

        public MessageOne(final int keyValue) {
            this.keyValue = Integer.valueOf(keyValue);
        }

        @MessageKey
        public Integer getKey() {
            return keyValue;
        }
    }

    @MessageType
    public static class MessageTwo {
        private final Integer keyValue;

        public MessageTwo(final int keyValue) {
            this.keyValue = Integer.valueOf(keyValue);
        }

        @MessageKey
        public Integer getKey() {
            return keyValue;
        }
    }

    @MessageType
    public static class ReturnString {
        public final String value;

        public ReturnString(final String value) {
            this.value = value;
        }

        @MessageKey
        public String getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value.equals(((ReturnString)o).value);
        }

    }

    @Mp
    public static class CombinedMP
        implements Cloneable {
        public long activationCount;
        public long activationTime;

        public long firstMessageTime = -1;
        public List<Object> messages; // note: can't be shared

        @Override
        public CombinedMP clone()
            throws CloneNotSupportedException {
            return (CombinedMP)super.clone();
        }

        @Activation
        public void activate(final byte[] data) {
            activationCount++;
            activationTime = System.nanoTime();

            messages = new ArrayList<Object>();
        }

        @MessageHandler
        public ReturnString handle(final MessageOne message) {
            if(firstMessageTime < 0)
                firstMessageTime = System.nanoTime();
            messages.add(message);
            return new ReturnString("MessageOne");
        }

        @MessageHandler
        public ReturnString handle(final MessageTwo message) {
            if(firstMessageTime < 0)
                firstMessageTime = System.nanoTime();
            messages.add(message);
            return new ReturnString("MessageTwo");
        }
    }

    @MessageType
    public static class ReturnInt {
        public final int value;

        public ReturnInt(final int value) {
            this.value = value;
        }

        @MessageKey
        public int getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value == ((ReturnInt)o).value;
        }
    }

    @Mp
    public static class OutputTestMP extends CombinedMP {
        public long outputTime;

        @Override
        public OutputTestMP clone() throws CloneNotSupportedException {
            return (OutputTestMP)super.clone();
        }

        @Output
        public ReturnInt doOutput() {
            outputTime = System.nanoTime();
            return new ReturnInt(42);
        }
    }

    @Mp
    public static class UnsuportedMessageTestMP implements Cloneable {
        @Override
        public UnsuportedMessageTestMP clone()
            throws CloneNotSupportedException {
            return (UnsuportedMessageTestMP)super.clone();
        }

        @MessageHandler
        public void handle(final MessageOne message) {
            // this method will never get called
        }
    }

    @MessageType
    public static class MessageWithNullKey {
        @MessageKey
        public Integer getKey() {
            return null;
        }
    }

    @Mp
    public static class NullKeyTestMP
        implements Cloneable {
        @Override
        public NullKeyTestMP clone()
            throws CloneNotSupportedException {
            return (NullKeyTestMP)super.clone();
        }

        @MessageHandler
        public void handle(final MessageWithNullKey message) {
            // this method will never get called
        }
    }

    public static class DummyDispatcher extends Dispatcher {
        public KeyedMessageWithType lastDispatched;
        public KeyExtractor ke;

        @Override
        public void dispatch(final KeyedMessageWithType message, final MessageResourceManager rm) {
            this.lastDispatched = message;
        }
    }

    private static KeyedMessageWithType km(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return new KeyExtractor().extract(message).get(0);
    }

    // ----------------------------------------------------------------------------
    // Test Cases
    // ----------------------------------------------------------------------------

    DummyDispatcher dispatcher;
    BasicClusterStatsCollector statsCollector;
    BasicNodeStatsCollector nodeStats;

    DefaultThreadingModel tm = null;

    @AfterEach
    public void tearDown() {
        if(tm != null)
            tm.close();
        tm = null;
    }

    @SuppressWarnings("resource")
    public SimpleContainer setupContainer(final MessageProcessorLifecycle<?> prototype) throws ContainerException {
        dispatcher = new DummyDispatcher();
        statsCollector = new BasicClusterStatsCollector();
        nodeStats = new BasicNodeStatsCollector();

        container = (SimpleContainer)new SimpleContainer().setMessageProcessor(prototype).setClusterId(new ClusterId("test", "test"));
        container.setDispatcher(dispatcher);
        container.setInbound(new DummyInbound());

        tm = new DefaultThreadingModel(TestInstanceManager.class.getName());
        tm.start(TestInstanceManager.class.getName());

        container.start(new TestInfrastructure(tm) {

            @Override
            public BasicClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
                return statsCollector;
            }

            @Override
            public NodeStatsCollector getNodeStatsCollector() {
                return nodeStats;
            }
        });
        return container;
    }

    @Test
    public void testSingleInstanceOneMessage() throws Throwable {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            assertEquals(0, manager.getProcessorCount(), "starts with no instances");

            final KeyedMessageWithType message = km(new MessageOne(123));
            final InstanceWrapper wrapper = manager.getInstanceForKey(message.key, message.message);
            assertEquals(1, manager.getProcessorCount(), "instance was created");

            final CombinedMP instance = (CombinedMP)wrapper.getInstance();
            // activation is now inline with insantiation so it's active immediately
            // assertEquals(0, instance.activationCount, "instance not already activated");
            assertEquals(1, instance.activationCount, "instance activated");
            assertEquals(-1, instance.firstMessageTime, "instance has no existing messages");
            // assertNull(instance.messages, "instance has no message list");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertEquals(0, instance.messages.size(), "message count");

            // dispatch the message
            // wrapper.run();
            manager.dispatch(message, Operation.handle, true);
            assertEquals(1, instance.activationCount, "instance activated");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertSame(message.message, instance.messages.get(0), "instance received message");
            assertEquals(1, instance.messages.size(), "message count");
            assertTrue(instance.activationTime < instance.firstMessageTime, "activated before first message");
            // The return value cannot be routed.
            assertEquals(new ReturnString("MessageOne"), ((DummyDispatcher)manager.getDispatcher()).lastDispatched.message);

            assertEquals(0, prototype.activationCount, "prototype not activated");
            assertEquals(-1, prototype.firstMessageTime, "prototype did not receive messages");
            assertNull(prototype.messages, "prototype has no message list");
        }
    }

    @Test
    public void testSingleInstanceTwoMessagesSameClassSeparateExecution()
        throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            assertEquals(0, manager.getProcessorCount(), "starts with no instances");

            final KeyedMessageWithType message1 = km(new MessageOne(123));
            final InstanceWrapper wrapper1 = manager.getInstanceForKey(message1.key, message1.message);
            manager.dispatch(message1, Operation.handle, true);
            final CombinedMP instance = (CombinedMP)wrapper1.getInstance();

            assertEquals(1, manager.getProcessorCount(), "instance was created");

            assertEquals(1, instance.activationCount, "instance activated");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertSame(message1.message, instance.messages.get(0), "instance received message");
            assertEquals(1, instance.messages.size(), "message count");
            assertTrue(instance.activationTime < instance.firstMessageTime, "activated before first message");
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

            final KeyedMessageWithType message2 = km(new MessageOne(123));
            final InstanceWrapper wrapper2 = manager.getInstanceForKey(message2.key, message2.message);
            manager.dispatch(message2, Operation.handle, true);
            assertSame(wrapper1, wrapper2, "same wrapper returned for second message");
            assertEquals(1, manager.getProcessorCount(), "no other instance was created");

            assertEquals(1, instance.activationCount, "no second activation");
            assertEquals(2, instance.messages.size(), "both messages delivered");
            assertSame(message1.message, instance.messages.get(0), "message1 delivered first");
            assertSame(message2.message, instance.messages.get(1), "message2 delivered second");
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
        }
    }

    @Test
    public void testSingleInstanceTwoMessagesSameClassCombinedExecution()
        throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            assertEquals(0, manager.getProcessorCount(), "starts with no instances");

            final KeyedMessageWithType message1 = km(new MessageOne(123));
            final InstanceWrapper wrapper = manager.getInstanceForKey(message1.key, message1.message);
            manager.dispatch(message1, Operation.handle, true);
            assertEquals(1, manager.getProcessorCount(), "instance was created");
            final KeyedMessageWithType message2 = km(new MessageOne(123));
            assertSame(wrapper, manager.getInstanceForKey(message2.key, message2.message),
                "same wrapper returned for second message");
            manager.dispatch(message2, Operation.handle, true);

            final CombinedMP instance = (CombinedMP)wrapper.getInstance();
            assertEquals(1, manager.getProcessorCount(), "no other instance was created");

            assertEquals(1, instance.activationCount, "instance activated");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertTrue(instance.activationTime < instance.firstMessageTime, "activated before first message");
            assertEquals(2, instance.messages.size(), "both messages delivered");
            assertSame(message1.message, instance.messages.get(0), "message1 delivered first");
            assertSame(message2.message, instance.messages.get(1), "message2 delivered second");
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
        }
    }

    @Test
    public void testSingleInstanceTwoMessagesDifferentClassSeparateExecution()
        throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            assertEquals(0, manager.getProcessorCount(), "starts with no instances");

            final KeyedMessageWithType message1 = km(new MessageOne(123));
            final InstanceWrapper wrapper = manager.getInstanceForKey(message1.key, message1.message);
            manager.dispatch(message1, Operation.handle, true);
            final CombinedMP instance = (CombinedMP)wrapper.getInstance();

            assertEquals(1, manager.getProcessorCount(), "instance was created");

            assertEquals(1, instance.activationCount, "instance activated");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertSame(message1.message, instance.messages.get(0), "instance received message");
            assertEquals(1, instance.messages.size(), "message count");
            assertTrue(instance.activationTime < instance.firstMessageTime, "activated before first message");
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

            final KeyedMessageWithType message2 = km(new MessageTwo(123));
            assertSame(wrapper, manager.getInstanceForKey(message2.key, message2.message), "same wrapper returned for second message");
            manager.dispatch(message2, Operation.handle, true);
            assertEquals(1, manager.getProcessorCount(), "no other instance was created");

            assertEquals(1, instance.activationCount, "no second activation");
            assertEquals(2, instance.messages.size(), "both messages delivered");
            assertSame(message1.message, instance.messages.get(0), "message1 delivered first");
            assertSame(message2.message, instance.messages.get(1), "message2 delivered second");
            assertEquals(new ReturnString("MessageTwo"), dispatcher.lastDispatched.message);
        }
    }

    @Test
    public void testMultipleInstanceCreation() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            assertEquals(0, manager.getProcessorCount(), "starts with no instances");

            final KeyedMessageWithType message1 = km(new MessageOne(123));
            final InstanceWrapper wrapper1 = manager.getInstanceForKey(message1.key, message1.message);
            manager.dispatch(message1, Operation.handle, true);
            final CombinedMP instance1 = (CombinedMP)wrapper1.getInstance();

            final KeyedMessageWithType message2 = km(new MessageOne(456));
            final InstanceWrapper wrapper2 = manager.getInstanceForKey(message2.key, message2.message);
            manager.dispatch(message2, Operation.handle, true);
            final CombinedMP instance2 = (CombinedMP)wrapper2.getInstance();

            assertEquals(2, manager.getProcessorCount(), "instances were created");

            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

            assertEquals(1, instance1.messages.size(), "message count to instance1");
            assertEquals(1, instance2.messages.size(), "message count to instance2");

            assertSame(message1.message, instance1.messages.get(0), "message1 went to instance1");
            assertSame(message2.message, instance2.messages.get(0), "message2 went to instance2");
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
        }
    }

    @Test
    public void testOutput() throws Exception {
        final OutputTestMP prototype = new OutputTestMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<OutputTestMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            // we need to dispatch messages to create MP instances
            final KeyedMessageWithType message1 = km(new MessageOne(1));
            final InstanceWrapper wrapper1 = manager.getInstanceForKey(message1.key, message1.message);
            manager.dispatch(message1, Operation.handle, true);
            final KeyedMessageWithType message2 = km(new MessageOne(2));
            final InstanceWrapper wrapper2 = manager.getInstanceForKey(message2.key, message2.message);
            manager.dispatch(message2, Operation.handle, true);
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

            manager.outputPass();

            final OutputTestMP mp1 = (OutputTestMP)wrapper1.getInstance();
            assertTrue(mp1.activationTime < mp1.outputTime, "MP1 output did not occur after activation");

            final OutputTestMP mp2 = (OutputTestMP)wrapper2.getInstance();
            assertTrue(mp2.activationTime < mp2.outputTime, "MP2 output did not occur after activation");
            assertTrue(mp1 != mp2);

            assertEquals(new ReturnInt(42), dispatcher.lastDispatched.message);
        }
    }

    @Test
    public void testOutputCountsOkay() throws Exception {
        final OutputTestMP prototype = new OutputTestMP();
        try(final Container manager = setupContainer(new MessageProcessor<OutputTestMP>(prototype));) {
            final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

            // we need to dispatch messages to create MP instances
            final KeyedMessageWithType message1 = km(new MessageOne(1));
            final KeyedMessageWithType message2 = km(new MessageOne(2));
            manager.dispatch(message1, Operation.handle, true);
            manager.dispatch(message2, Operation.handle, true);
            assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

            manager.invokeOutput();
            assertEquals(4,
                ((ClusterMetricGetters)statsCollector).getProcessedMessageCount(),
                "number of processed messages should include outputs.");
        }
    }

    @Test
    public void testOutputShortCircuitsIfNoOutputMethod() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

        // we need to dispatch messages to create MP instances
        final KeyedMessageWithType message1 = km(new MessageOne(1));
        final KeyedMessageWithType message2 = km(new MessageOne(2));
        manager.dispatch(message1, Operation.handle, true);
        manager.dispatch(message2, Operation.handle, true);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        manager.invokeOutput();
        // output messages are NOT considered "processed" if there is no output method on the MP.
        assertEquals(2,
            ((ClusterMetricGetters)statsCollector).getProcessedMessageCount(),
            "number of processed messages should include outputs.");
    }

    // This test no longer really matters since there is no queue but we might as well leave it
    // since it exercises the container.
    @Test
    public void testQueueIsClearedAfterExecution() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {

            final KeyedMessageWithType message = km(new MessageOne(123));
            final InstanceWrapper wrapper = manager.getInstanceForKey(message.key, message.message);
            manager.dispatch(message, Operation.handle, true);
            assertEquals(1, manager.getProcessorCount(), "instance was created");

            final CombinedMP instance = (CombinedMP)wrapper.getInstance();

            assertEquals(1, instance.activationCount, "instance activated");
            assertTrue(instance.activationTime > 0, "real activation time");
            assertSame(message.message, instance.messages.get(0), "instance received message");
            assertEquals(1, instance.messages.size(), "message count");
            assertTrue(instance.activationTime < instance.firstMessageTime, "activated before first message");

            final long activationTime = instance.activationTime;
            final long firstMessageTime = instance.firstMessageTime;

            // here is where the queue would have been advanced again ... but there is no queue anymore.
            assertTrue(activationTime == instance.activationTime, "activation time didn't change");
            assertTrue(firstMessageTime == instance.firstMessageTime, "message time didn't change");
            assertEquals(1, instance.messages.size(), "message count didn't change");
        }
    }

    @Test
    public void testFailureNullMessage() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        try(final SimpleContainer manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));) {
            assertThrows(NullPointerException.class, () -> manager.getInstanceForKey(null, null));
        }
    }

    @Test
    public void testFailureNoKeyMethod() throws Exception {
        try(final SimpleContainer dispatcher = setupContainer(new MessageProcessor<NullKeyTestMP>(new NullKeyTestMP()));) {
            final KeyedMessageWithType message = km(new MessageWithNullKey());
            assertThrows(NullPointerException.class, () -> dispatcher.getInstanceForKey(message.key, message.message));
        }
    }

    @Mp
    public static class ThrowMe implements Cloneable {
        @MessageHandler
        public void handle(final MessageOne message) {
            throw new RuntimeException("YO!");
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Test
    public void testMpThrows() throws Exception {
        try(final SimpleContainer dispatcher = setupContainer(new MessageProcessor<ThrowMe>(new ThrowMe()));) {

            dispatcher.dispatch(km(new MessageOne(123)), Operation.handle, true);

            assertEquals(1, ((ClusterMetricGetters)statsCollector).getMessageFailedCount());
        }
    }
}
