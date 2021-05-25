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

package net.dempsy.container.nonlocking;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import net.dempsy.config.ClusterId;
import net.dempsy.container.ClusterMetricGetters;
import net.dempsy.container.Container;
import net.dempsy.container.Container.Operation;
import net.dempsy.container.ContainerException;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.container.mocks.DummyInbound;
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
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.basic.BasicClusterStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.util.TestInfrastructure;
import net.dempsy.utils.test.CloseableRule;

public class TestInstanceManager {

    private Container manager;

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
    DefaultThreadingModel tm = null;

    @Rule public CloseableRule t = new CloseableRule(() -> {
        if(tm != null)
            tm.close();
    });

    @SuppressWarnings("resource")
    public Container setupContainer(final MessageProcessorLifecycle<?> prototype) throws ContainerException {
        dispatcher = new DummyDispatcher();
        statsCollector = new BasicClusterStatsCollector();

        manager = new NonLockingAltContainer().setMessageProcessor(prototype).setClusterId(new ClusterId("test", "test"));
        manager.setDispatcher(dispatcher);
        manager.setInbound(new DummyInbound());

        tm = new DefaultThreadingModel(TestInstanceManager.class.getName());
        tm.start(TestInstanceManager.class.getName());

        manager.start(new TestInfrastructure(tm) {
            BasicNodeStatsCollector nStats = new BasicNodeStatsCollector();

            @Override
            public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
                return statsCollector;
            }

            @Override
            public NodeStatsCollector getNodeStatsCollector() {
                return nStats;
            }

        });
        return manager;
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
            assertEquals("number of processed messages should include outputs.", 4,
                ((ClusterMetricGetters)statsCollector).getProcessedMessageCount());
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
        assertEquals("number of processed messages should include outputs.", 2,
            ((ClusterMetricGetters)statsCollector).getProcessedMessageCount());
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
        try(final Container dispatcher = setupContainer(new MessageProcessor<ThrowMe>(new ThrowMe()));) {

            dispatcher.dispatch(km(new MessageOne(123)), Operation.handle, true);

            assertEquals(1, ((ClusterMetricGetters)statsCollector).getMessageFailedCount());
        }
    }
}
