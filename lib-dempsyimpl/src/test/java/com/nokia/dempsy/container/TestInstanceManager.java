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

package com.nokia.dempsy.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer.InstanceWrapper;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.coda.MetricGetters;
import com.nokia.dempsy.monitoring.coda.StatsCollectorCoda;
import com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda;
import com.nokia.dempsy.serialization.java.JavaSerializer;


public class TestInstanceManager
{
   
   private MpContainer manager;
   
//----------------------------------------------------------------------------
//  Test classes -- must be static/public for introspection
//----------------------------------------------------------------------------

   public static class MessageOne
   {
      private Integer keyValue;

      public MessageOne(int keyValue)
      {
         this.keyValue = Integer.valueOf(keyValue);
      }

      @MessageKey
      public Integer getKey()
      {
         return keyValue;
      }
   }


   public static class MessageTwo
   {
      private Integer keyValue;

      public MessageTwo(int keyValue)
      {
         this.keyValue = Integer.valueOf(keyValue);
      }

      @MessageKey
      public Integer getKey()
      {
         return keyValue;
      }
   }


   @MessageProcessor
   public static class CombinedMP
   implements Cloneable
   {
      public long activationCount;
      public long activationTime;

      public long firstMessageTime = -1;
      public List<Object> messages; // note: can't be shared

      @Override
      public CombinedMP clone()
      throws CloneNotSupportedException
      {
         return (CombinedMP)super.clone();
      }

      @Activation
      public void activate(byte[] data)
      {
         activationCount++;
         activationTime = System.nanoTime();

         messages = new ArrayList<Object>();
      }

      @MessageHandler
      public String handle(MessageOne message)
      {
         if (firstMessageTime < 0)
            firstMessageTime = System.nanoTime();
         messages.add(message);
         return "MessageOne";
      }

      @MessageHandler
      public String handle(MessageTwo message)
      {
         if (firstMessageTime < 0)
            firstMessageTime = System.nanoTime();
         messages.add(message);
         return "MessageTwo";
      }
   }


   @MessageProcessor
   public static class OutputTestMP
   extends CombinedMP
   {
      public long outputTime;

      @Override
      public OutputTestMP clone()
      throws CloneNotSupportedException
      {
         return (OutputTestMP)super.clone();
      }

      @Output
      public int doOutput()
      {
         outputTime = System.nanoTime();
         return 42;
      }
   }


   @MessageProcessor
   public static class UnsuportedMessageTestMP
   implements Cloneable
   {
      @Override
      public UnsuportedMessageTestMP clone()
      throws CloneNotSupportedException
      {
         return (UnsuportedMessageTestMP)super.clone();
      }

      @MessageHandler
      public void handle(MessageOne message)
      {
         // this method will never get called
      }
   }


   public static class MessageWithNullKey
   {
      @MessageKey
      public Integer getKey()
      {
         return null;
      }
   }


   @MessageProcessor
   public static class NullKeyTestMP
   implements Cloneable
   {
      @Override
      public NullKeyTestMP clone()
      throws CloneNotSupportedException
      {
         return (NullKeyTestMP)super.clone();
      }

      @MessageHandler
      public void handle(MessageWithNullKey message)
      {
         // this method will never get called
      }
   }

   public static class DummyDispatcher implements Dispatcher
   {
      public Object lastDispatched;

      @Override
      public void dispatch(Object message)
      {
         this.lastDispatched = message;
      }
   }


//----------------------------------------------------------------------------
//  Test Cases
//----------------------------------------------------------------------------
   
   public MpContainer setupContainer(Object prototype) throws ContainerException
   {
      DummyDispatcher dispatcher = new DummyDispatcher();
      StatsCollector stats = new StatsCollectorCoda(new ClusterId("test", "test"), new StatsCollectorFactoryCoda().getNamingStrategy());
      JavaSerializer<Object> serializer = new JavaSerializer<Object>();

      manager = new MpContainer(new ClusterId("test","test"));
      manager.setDispatcher(dispatcher);
      manager.setStatCollector(stats);
      manager.setSerializer(serializer);
      manager.setPrototype(prototype);
      return manager;
   }
   
   @After
   public void tearDown()
   {
      // Destroy any counts, so we start fresh on the next test
      StatsCollector stats = manager.getStatsCollector();
      if (stats != null)
      {
         ((StatsCollectorCoda)stats).stop();
      }
   }

   @Test
   public void testSingleInstanceOneMessage() throws Throwable
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);
      
      assertEquals("starts with no instances", 0, manager.getProcessorCount());

      MessageOne message = new MessageOne(123);
      InstanceWrapper wrapper = manager.getInstanceForDispatch(message);
      assertEquals("instance was created", 1, manager.getProcessorCount());

      CombinedMP instance = (CombinedMP)wrapper.getInstance();
      // activation is now inline with insantiation so it's active immediately
//      assertEquals("instance not already activated", 0, instance.activationCount);
      assertEquals("instance activated", 1, instance.activationCount);
      assertEquals("instance has no existing messages", -1, instance.firstMessageTime);
//      assertNull("instance has no message list", instance.messages);
      assertTrue("real activation time", instance.activationTime > 0);
      assertEquals("message count", 0, instance.messages.size());

      // dispatch the message
//      wrapper.run();
      manager.dispatch(message, true);
      assertEquals("instance activated", 1, instance.activationCount);
      assertTrue("real activation time", instance.activationTime > 0);
      assertSame("instance received message", message, instance.messages.get(0));
      assertEquals("message count", 1, instance.messages.size());
      assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
      assertEquals("MessageOne",((DummyDispatcher)manager.getDispatcher()).lastDispatched);

      assertEquals("prototype not activated", 0, prototype.activationCount);
      assertEquals("prototype did not receive messages", -1, prototype.firstMessageTime);
      assertNull("prototype has no message list", prototype.messages);
   }


   @Test
   public void testSingleInstanceTwoMessagesSameClassSeparateExecution()
   throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());
      
      assertEquals("starts with no instances", 0, manager.getProcessorCount());

      MessageOne message1 = new MessageOne(123);
      InstanceWrapper wrapper1 = manager.getInstanceForDispatch(message1);
      manager.dispatch(message1, false);
      CombinedMP instance = (CombinedMP)wrapper1.getInstance();

      assertEquals("instance was created", 1, manager.getProcessorCount());

      assertEquals("instance activated", 1, instance.activationCount);
      assertTrue("real activation time", instance.activationTime > 0);
      assertSame("instance received message", message1, instance.messages.get(0));
      assertEquals("message count", 1, instance.messages.size());
      assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
      assertEquals("MessageOne",dispatcher.lastDispatched);

      MessageOne message2 = new MessageOne(123);
      InstanceWrapper wrapper2 = manager.getInstanceForDispatch(message2);
      manager.dispatch(message2, false);
      assertSame("same wrapper returned for second message", wrapper1, wrapper2);
      assertEquals("no other instance was created", 1, manager.getProcessorCount());

      assertEquals("no second activation", 1, instance.activationCount);
      assertEquals("both messages delivered", 2, instance.messages.size());
      assertSame("message1 delivered first", message1, instance.messages.get(0));
      assertSame("message2 delivered second", message2, instance.messages.get(1));
      assertEquals("MessageOne",dispatcher.lastDispatched);
   }


   @Test
   public void testSingleInstanceTwoMessagesSameClassCombinedExecution()
   throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

      assertEquals("starts with no instances", 0, manager.getProcessorCount());

      MessageOne message1 = new MessageOne(123);
      InstanceWrapper wrapper = manager.getInstanceForDispatch(message1);
      manager.dispatch(message1,false);
      assertEquals("instance was created", 1, manager.getProcessorCount());
      MessageOne message2 = new MessageOne(123);
      assertSame("same wrapper returned for second message",
                 wrapper, manager.getInstanceForDispatch(message2));
      manager.dispatch(message2, false);

      CombinedMP instance = (CombinedMP)wrapper.getInstance();
      assertEquals("no other instance was created", 1, manager.getProcessorCount());

      assertEquals("instance activated", 1, instance.activationCount);
      assertTrue("real activation time", instance.activationTime > 0);
      assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
      assertEquals("both messages delivered", 2, instance.messages.size());
      assertSame("message1 delivered first", message1, instance.messages.get(0));
      assertSame("message2 delivered second", message2, instance.messages.get(1));
      assertEquals("MessageOne",dispatcher.lastDispatched);
   }


   @Test
   public void testSingleInstanceTwoMessagesDifferentClassSeparateExecution()
   throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

      assertEquals("starts with no instances", 0, manager.getProcessorCount());

      MessageOne message1 = new MessageOne(123);
      InstanceWrapper wrapper = manager.getInstanceForDispatch(message1);
      manager.dispatch(message1, true);
      CombinedMP instance = (CombinedMP)wrapper.getInstance();

      assertEquals("instance was created", 1, manager.getProcessorCount());

      assertEquals("instance activated", 1, instance.activationCount);
      assertTrue("real activation time", instance.activationTime > 0);
      assertSame("instance received message", message1, instance.messages.get(0));
      assertEquals("message count", 1, instance.messages.size());
      assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
      assertEquals("MessageOne",dispatcher.lastDispatched);

      MessageTwo message2 = new MessageTwo(123);
      assertSame("same wrapper returned for second message",
                 wrapper, manager.getInstanceForDispatch(message2));
      manager.dispatch(message2, false);
      assertEquals("no other instance was created", 1, manager.getProcessorCount());

      assertEquals("no second activation", 1, instance.activationCount);
      assertEquals("both messages delivered", 2, instance.messages.size());
      assertSame("message1 delivered first", message1, instance.messages.get(0));
      assertSame("message2 delivered second", message2, instance.messages.get(1));
      assertEquals("MessageTwo",dispatcher.lastDispatched);
   }


   @Test
   public void testMultipleInstanceCreation() throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)manager.getDispatcher());

      assertEquals("starts with no instances", 0, manager.getProcessorCount());

      MessageOne message1 = new MessageOne(123);
      InstanceWrapper wrapper1 = manager.getInstanceForDispatch(message1);
      manager.dispatch(message1, true);
      CombinedMP instance1 = (CombinedMP)wrapper1.getInstance();

      MessageOne message2 = new MessageOne(456);
      InstanceWrapper wrapper2 = manager.getInstanceForDispatch(message2);
      manager.dispatch(message2, false);
      CombinedMP instance2 = (CombinedMP)wrapper2.getInstance();

      assertEquals("instances were created", 2, manager.getProcessorCount());

      assertEquals("MessageOne",dispatcher.lastDispatched);

      assertEquals("message count to instance1", 1, instance1.messages.size());
      assertEquals("message count to instance2", 1, instance2.messages.size());

      assertSame("message1 went to instance1", message1, instance1.messages.get(0));
      assertSame("message2 went to instance2", message2, instance2.messages.get(0));
      assertEquals("MessageOne",dispatcher.lastDispatched);
   }


   @Test
   public void testOutput() throws Exception
   {
      OutputTestMP prototype = new OutputTestMP();
      MpContainer instanceManager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)instanceManager.getDispatcher());

      // we need to dispatch messages to create MP instances
      MessageOne message1 = new MessageOne(1);
      InstanceWrapper wrapper1 = instanceManager.getInstanceForDispatch(message1);
      instanceManager.dispatch(message1, true);
      MessageOne message2 = new MessageOne(2);
      InstanceWrapper wrapper2 = instanceManager.getInstanceForDispatch(message2);
      instanceManager.dispatch(message2, true);
      assertEquals("MessageOne",dispatcher.lastDispatched);

      instanceManager.outputPass();

      OutputTestMP mp1 = (OutputTestMP)wrapper1.getInstance();
      assertTrue("MP1 output did not occur after activation", mp1.activationTime < mp1.outputTime);

      OutputTestMP mp2 = (OutputTestMP)wrapper2.getInstance();
      assertTrue("MP2 output did not occur after activation", mp2.activationTime < mp2.outputTime);
      assertTrue(mp1 != mp2);

      assertEquals(new Integer(42),dispatcher.lastDispatched);
   }


   @Test
   public void testOutputShortCircuitsIfWrongClass() throws Exception
   {
      OutputTestMP prototype = new OutputTestMP();
      MpContainer instanceManager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)instanceManager.getDispatcher());

      // we need to dispatch messages to create MP instances
      MessageOne message1 =new MessageOne(1);
      MessageOne message2 =new MessageOne(2);
      instanceManager.dispatch(message1, true);
      instanceManager.dispatch(message2, false);
      assertEquals("MessageOne",dispatcher.lastDispatched);
      
      instanceManager.outputPass();
      assertEquals("number of processed messages should include outputs.", 4, ((MetricGetters)instanceManager.getStatsCollector()).getProcessedMessageCount());
   }


   @Test
   public void testOutputShortCircuitsIfNoOutputMethod() throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer instanceManager = setupContainer(prototype);
      DummyDispatcher dispatcher = ((DummyDispatcher)instanceManager.getDispatcher());

      // we need to dispatch messages to create MP instances
      MessageOne message1 =new MessageOne(1);
      MessageOne message2 =new MessageOne(2);
      instanceManager.dispatch(message1, true);
      instanceManager.dispatch(message2, false);
      assertEquals("MessageOne",dispatcher.lastDispatched);
      
      instanceManager.outputPass();
      // output messages are NOT considered "processed" if there is no output method on the MP.
      assertEquals("number of processed messages should include outputs.", 2, ((MetricGetters)instanceManager.getStatsCollector()).getProcessedMessageCount());
   }

   // This test no longer really matters since there is no queue but we might as well leave it
   //  since it exercises the container.
   @Test
   public void testQueueIsClearedAfterExecution() throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);

      MessageOne message = new MessageOne(123);
      InstanceWrapper wrapper = manager.getInstanceForDispatch(message);
      manager.dispatch(message, false);
      assertEquals("instance was created", 1, manager.getProcessorCount());

      CombinedMP instance = (CombinedMP)wrapper.getInstance();

      assertEquals("instance activated", 1, instance.activationCount);
      assertTrue("real activation time", instance.activationTime > 0);
      assertSame("instance received message", message, instance.messages.get(0));
      assertEquals("message count", 1, instance.messages.size());
      assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);

      long activationTime = instance.activationTime;
      long firstMessageTime = instance.firstMessageTime;

// here is where the queue would have been advanced again ... but there is no queue anymore.
      assertTrue("activation time didn't change", activationTime == instance.activationTime);
      assertTrue("message time didn't change", firstMessageTime == instance.firstMessageTime);
      assertEquals("message count didn't change", 1, instance.messages.size());
   }


   @Test(expected=ContainerException.class)
   public void testFailureNullMessage() throws Exception
   {
      CombinedMP prototype = new CombinedMP();
      MpContainer manager = setupContainer(prototype);

      manager.getInstanceForDispatch(null);
   }


   @Test(expected=ContainerException.class)
   public void testFailureUnsupportedMessage() throws Exception
   {
      MpContainer dispatcher = setupContainer(new UnsuportedMessageTestMP());
      dispatcher.getInstanceForDispatch(new MessageTwo(123));
   }


   @Test(expected=ContainerException.class)
   public void testFailureNoKeyMethod() throws Exception
   {
      MpContainer dispatcher = setupContainer(new NullKeyTestMP());
      dispatcher.getInstanceForDispatch(new MessageWithNullKey());
   }
   
   @MessageProcessor
   public static class ThrowMe implements Cloneable
   {
      @MessageHandler
      public void handle(MessageOne message)
      {
         throw new RuntimeException("YO!");
      }
      
      @Override
      public Object clone() throws CloneNotSupportedException
      {
         return super.clone();
      }

   }
   
   @Test
   public void testMpThrows() throws Exception
   {
      MpContainer dispatcher = setupContainer(new ThrowMe());
      
      dispatcher.dispatch(new MessageOne(123), true);
      
      assertEquals(1,((MetricGetters)dispatcher.getStatsCollector()).getMessageFailedCount());
   }
}
