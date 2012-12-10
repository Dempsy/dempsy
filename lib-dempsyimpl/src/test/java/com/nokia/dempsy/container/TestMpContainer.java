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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.KeySource;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.Evictable;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Passivation;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.mocks.ContainerTestMessage;
import com.nokia.dempsy.container.mocks.OutputMessage;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.blockingqueue.BlockingQueueAdaptor;
import com.nokia.dempsy.monitoring.coda.MetricGetters;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.serialization.Serializer;
import com.nokia.dempsy.serialization.java.JavaSerializer;

//
// NOTE: this test simply puts messages on an input queue, and expects
//       messages on an output queue; the important part is the wiring
//       in TestMPContainer.xml
//
public class TestMpContainer
{
//----------------------------------------------------------------------------
//  Configuration
//----------------------------------------------------------------------------

   private MpContainer container;
   private BlockingQueue<Object> inputQueue;
   private BlockingQueue<Object> outputQueue;
   private Serializer<Object> serializer = new JavaSerializer<Object>();

   private ClassPathXmlApplicationContext context;
   private long baseTimeoutMillis = 2000;

   public static class DummyDispatcher implements Dispatcher
   {
      public Object lastDispatched;
      
      public Sender sender;
      
      public Serializer<Object> serializer = new JavaSerializer<Object>();

      @Override
      public void dispatch(Object message)
      {
         this.lastDispatched = message;
         try
         {
            sender.send(serializer.serialize(message));
         }
         catch(Exception e)
         {
            System.out.println("FAILED!");
            e.printStackTrace();
            throw new RuntimeException(e);
         }
      }
      
      @Override
      public ClusterId getThisClusterId() { return null; }
      
      public void setSender(Sender sender)
      {
         this.sender = sender;
      }
   }


   @Before
   public void setUp() throws Throwable { setUp("true"); }
   
   @SuppressWarnings("unchecked")
   public void setUp(String failFast) throws Exception
   {
      System.setProperty("failFast", failFast);
      context = new ClassPathXmlApplicationContext("TestMPContainer.xml");
      container = (MpContainer)context.getBean("container");
      assertNotNull(container.getSerializer());
      inputQueue = (BlockingQueue<Object>)context.getBean("inputQueue");
      outputQueue = (BlockingQueue<Object>)context.getBean("outputQueue");
   }


   @After
   public void tearDown() throws Exception
   {
      container.shutdown();
      context.close();
      context.destroy();
      context = null;
      container = null;
      inputQueue = null;
      outputQueue = null;
   }


//----------------------------------------------------------------------------
//  Message and MP classes
//----------------------------------------------------------------------------



   @MessageProcessor
   public static class TestProcessor implements Cloneable
   {
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
      public void startMe(ClusterId clusterId) { this.clusterId = clusterId; startCalled.incrementAndGet(); }

      @Override
      public TestProcessor clone()
      throws CloneNotSupportedException
      {
         cloneCount.incrementAndGet();
         return (TestProcessor)super.clone();
      }

      @Activation
      public void activate(byte[] data)
      {
         activationCount++;
      }
      
      @Passivation
      public void passivate() throws InterruptedException
      {
         passivateCount.incrementAndGet();
         
         blockPassivate.await();
         
         if (throwPassivateException.get())
         {
            passivateExceptionCount.incrementAndGet();
            throw new RuntimeException("Passivate");
         }
      }

      @MessageHandler
      public ContainerTestMessage handle(ContainerTestMessage message) throws InterruptedException
      {
         myKey = message.getKey();
         invocationCount++;
         
         latch.await();

         // put it on output queue so that we can synchronize test
         return message;
      }
      
      @Evictable
      public boolean isEvictable(){ return evict.get(); }

      @Output
      public OutputMessage doOutput() throws InterruptedException
      {
         numOutputExecutions.incrementAndGet();
         try
         {
            blockAllOutput.await();
            return new OutputMessage(myKey, activationCount, invocationCount, outputCount++);
         }
         finally
         {
            numOutputExecutions.decrementAndGet();
         }
      }
   }


//----------------------------------------------------------------------------
//  Test Cases
//----------------------------------------------------------------------------

   @Test
   public void testConfiguration() throws Exception
   {
      // this assertion is superfluous, since we deref container in setUp()
      assertNotNull("did not create container", container);
      
      TestProcessor prototype = context.getBean(TestProcessor.class);
      assertEquals(1,prototype.startCalled.get());
      
      assertNotNull(prototype.clusterId);
   }


   @Test
   public void testMessageDispatch()
   throws Exception
   {
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      outputQueue.poll(1000, TimeUnit.MILLISECONDS);

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);

      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      outputQueue.poll(1000, TimeUnit.MILLISECONDS);

      assertEquals("activation count, 2nd message", 1, mp.activationCount);
      assertEquals("invocation count, 2nd message", 2, mp.invocationCount);
   }


   @Test
   public void testInvokeOutput()
   throws Exception
   {
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      outputQueue.poll(1000, TimeUnit.MILLISECONDS);
      inputQueue.add(serializer.serialize(new ContainerTestMessage("bar")));
      outputQueue.poll(1000, TimeUnit.MILLISECONDS);

      assertEquals("number of MP instances", 2, container.getProcessorCount());
      assertTrue("queue is empty", outputQueue.isEmpty());

      container.outputPass();
      OutputMessage out1 = (OutputMessage)serializer.deserialize((byte[]) outputQueue.poll(1000, TimeUnit.MILLISECONDS));
      OutputMessage out2 = (OutputMessage)serializer.deserialize((byte[]) outputQueue.poll(1000, TimeUnit.MILLISECONDS));

      assertTrue("messages received", (out1 != null) && (out2 != null));
      assertEquals("no more messages in queue", 0, outputQueue.size());

      // order of messages is not guaranteed, so we need to aggregate keys
      HashSet<String> messageKeys = new HashSet<String>();
      messageKeys.add(out1.getKey());
      messageKeys.add(out2.getKey());
      assertTrue("first MP sent output", messageKeys.contains("foo"));
      assertTrue("second MP sent output", messageKeys.contains("bar"));
   }

   @Test
   public void testOutputInvoker() throws Exception {
     inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
        ContainerTestMessage out1 = (ContainerTestMessage)serializer.deserialize((byte[]) outputQueue.poll(1000, TimeUnit.MILLISECONDS));
        assertTrue("messages received", (out1 != null) );

        assertEquals("number of MP instances", 1, container.getProcessorCount());
        assertTrue("queue is empty", outputQueue.isEmpty());

   }
   
   @Test
   public void testMtInvokeOutput()
   throws Exception
   {
      final int numInstances = 20;
      final int concurrency = 5;
      
      container.setConcurrency(concurrency);
      
      for (int i = 0; i < numInstances; i++)
         inputQueue.put(serializer.serialize(new ContainerTestMessage("foo" + i)));
      for (int i = 0; i < numInstances; i++)
         assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("number of MP instances", numInstances, container.getProcessorCount());
      assertTrue("queue is empty", outputQueue.isEmpty());

      container.outputPass();
      for (int i = 0; i < numInstances; i++)
         assertNotNull(serializer.deserialize((byte[]) outputQueue.poll(1000, TimeUnit.MILLISECONDS)));

      assertEquals("no more messages in queue", 0, outputQueue.size());
   }

   @Test
   public void testEvictable() throws Exception {
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);

      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("activation count, 2nd message", 1, mp.activationCount);
      assertEquals("invocation count, 2nd message", 2, mp.invocationCount);
      TestProcessor prototype = context.getBean(TestProcessor.class);
      int tmpCloneCount = prototype.cloneCount.intValue();
      
      mp.evict.set(true);
      container.evict();
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("Clone count, 2nd message", tmpCloneCount+1, prototype.cloneCount.intValue());
   }
   
   @Test
   public void testEvictableWithPassivateException() throws Exception 
   {
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);
      mp.throwPassivateException.set(true);

      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("activation count, 2nd message", 1, mp.activationCount);
      assertEquals("invocation count, 2nd message", 2, mp.invocationCount);
      TestProcessor prototype = context.getBean(TestProcessor.class);
      int tmpCloneCount = prototype.cloneCount.intValue();
      
      mp.evict.set(true);
      container.evict();
      assertEquals("Passivate Exception Thrown",1, mp.passivateExceptionCount.get());
      
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("Clone count, 2nd message", tmpCloneCount+1, prototype.cloneCount.intValue());
   }

   @Test
   public void testEvictableWithBusyMp() throws Throwable
   {
      // first set the receiver to failFast
      
      // This forces the instantiation of an Mp
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      // Once the poll finishes the Mp is instantiated and handling messages.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);
      
      // now we're going to cause the processing to be held up.
      mp.latch = new CountDownLatch(1);
      mp.evict.set(true); // allow eviction

      // sending it a message will now cause it to hang up while processing
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      
      TestProcessor prototype = context.getBean(TestProcessor.class);

      // keep track of the cloneCount for later checking
      int tmpCloneCount = prototype.cloneCount.intValue();
      
      // invocation count should go to 2
      assertTrue(TestUtils.poll(baseTimeoutMillis, mp, new TestUtils.Condition<TestProcessor>() 
            { @Override public boolean conditionMet(TestProcessor o) { return o.invocationCount == 2; } }));
      
      assertNull(outputQueue.peek()); // this is a double check that no message got all the way 
      
      // now kick off the evict in a separate thread since we expect it to hang
      // until the mp becomes unstuck.
      final AtomicBoolean evictIsComplete = new AtomicBoolean(false); // this will allow us to see the evict pass complete
      Thread thread = new Thread(new Runnable() { @Override public void run() { container.evict(); evictIsComplete.set(true); } });
      thread.start();
      
      // now check to make sure eviction doesn't complete.
      Thread.sleep(50); // just a little to give any mistakes a change to work themselves through
      assertFalse(evictIsComplete.get()); // make sure eviction didn't finish
      
      mp.latch.countDown(); // this lets it go
      
      // wait until the eviction completes
      assertTrue(TestUtils.poll(baseTimeoutMillis, evictIsComplete, new TestUtils.Condition<AtomicBoolean>() 
            { @Override public boolean conditionMet(AtomicBoolean o) { return o.get(); } }));

      // now it comes through.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("activation count, 2nd message", 1, mp.activationCount);
      assertEquals("invocation count, 2nd message", 2, mp.invocationCount);
      
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("Clone count, 2nd message", tmpCloneCount+1, prototype.cloneCount.intValue());
   }

   @Test
   public void testEvictCollision() throws Throwable
   {
      // This forces the instantiation of an Mp
      BlockingQueueAdaptor adaptor = context.getBean(BlockingQueueAdaptor.class);
      assertNotNull(adaptor);
      adaptor.setFailFast(true);
      
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      // Once the poll finishes the Mp is instantiated and handling messages.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);
      
      // now we're going to cause the passivate to be held up.
      mp.blockPassivate = new CountDownLatch(1);
      mp.evict.set(true); // allow eviction

      // now kick off the evict in a separate thread since we expect it to hang
      // until the mp becomes unstuck.
      final AtomicBoolean evictIsComplete = new AtomicBoolean(false); // this will allow us to see the evict pass complete
      Thread thread = new Thread(new Runnable() { @Override public void run() { container.evict(); evictIsComplete.set(true); } });
      thread.start();
      
      Thread.sleep(50); //let it get going.
      assertFalse(evictIsComplete.get()); // check to see we're hung.

      final MetricGetters sc = (MetricGetters)container.getStatsCollector();
      assertEquals(0,sc.getMessageCollisionCount());
      
      // sending it a message will now cause it to have the collision tick up
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, sc, new TestUtils.Condition<MetricGetters>() 
            { @Override public boolean conditionMet(MetricGetters o) { return o.getMessageCollisionCount() == 1; } }));
      
      // now let the evict finish
      mp.blockPassivate.countDown();
      
      // wait until the eviction completes
      assertTrue(TestUtils.poll(baseTimeoutMillis, evictIsComplete, new TestUtils.Condition<AtomicBoolean>() 
            { @Override public boolean conditionMet(AtomicBoolean o) { return o.get(); } }));
      
      // invocationCount should still be 1 from the initial invocation that caused the clone
      assertEquals(1,mp.invocationCount);

      // send a message that should go through
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));

      // Once the poll finishes a new Mp is instantiated and handling messages.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      TestProcessor mp2 = (TestProcessor)container.getMessageProcessor("foo");

      // make sure this new Mp isn't the old one.
      assertTrue(mp != mp2);

   }

   @Test
   public void testEvictCollisionWithBlocking() throws Throwable
   {
      // for this test we need to undo the setup
      tearDown();
      
      // and re-setUp setting failFast to false.
      setUp("false");
      
      // This forces the instantiation of an Mp
      BlockingQueueAdaptor adaptor = context.getBean(BlockingQueueAdaptor.class);
      assertNotNull(adaptor);
      
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      // Once the poll finishes the Mp is instantiated and handling messages.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals("did not create MP", 1, container.getProcessorCount());

      TestProcessor mp = (TestProcessor)container.getMessageProcessor("foo");
      assertNotNull("MP not associated with expected key", mp);
      assertEquals("activation count, 1st message", 1, mp.activationCount);
      assertEquals("invocation count, 1st message", 1, mp.invocationCount);
      
      // now we're going to cause the passivate to be held up.
      mp.blockPassivate = new CountDownLatch(1);
      mp.evict.set(true); // allow eviction

      // now kick off the evict in a separate thread since we expect it to hang
      // until the mp becomes unstuck.
      final AtomicBoolean evictIsComplete = new AtomicBoolean(false); // this will allow us to see the evict pass complete
      Thread thread = new Thread(new Runnable() { @Override public void run() { container.evict(); evictIsComplete.set(true); } });
      thread.start();
      
      Thread.sleep(500); //let it get going.
      assertFalse(evictIsComplete.get()); // check to see we're hung.

      final MetricGetters sc = (MetricGetters)container.getStatsCollector();
      assertEquals(0,sc.getMessageCollisionCount());
      
      // sending it a message will now cause it to have the collision tick up
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));
      
      // give it some time.
      Thread.sleep(100);
      
      // make sure there's no collision
      assertEquals(0,sc.getMessageCollisionCount());
      
      // make sure the message didn't get through
      assertNull(outputQueue.peek());
      
      // make sure no message got handled
      assertEquals(1,mp.invocationCount); // 1 is the initial invocation that caused the instantiation.
      
      // now let the evict finish
      mp.blockPassivate.countDown();
      
      // wait until the eviction completes
      assertTrue(TestUtils.poll(baseTimeoutMillis, evictIsComplete, new TestUtils.Condition<AtomicBoolean>() 
            { @Override public boolean conditionMet(AtomicBoolean o) { return o.get(); } }));

      // Once the poll finishes a new Mp is instantiated and handling messages.
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      // get the new Mp
      TestProcessor mp2 = (TestProcessor)container.getMessageProcessor("foo");

      // invocationCount should be 1 from the initial invocation that caused the clone, and no more
      assertEquals(1,mp.invocationCount);
      assertEquals(1,mp2.invocationCount);
      assertTrue(mp != mp2);

      // send a message that should go through
      inputQueue.add(serializer.serialize(new ContainerTestMessage("foo")));

      // Once the poll finishes mp2 invocationCount should be incremented
      assertNotNull(outputQueue.poll(baseTimeoutMillis, TimeUnit.MILLISECONDS));

      assertEquals(1,mp.invocationCount);
      assertEquals(2,mp2.invocationCount);
   }
   
   static class FixedInbound implements RoutingStrategy.Inbound
   {
      static AtomicReference<Set<Object>> validKeys = new AtomicReference<Set<Object>>(new HashSet<Object>());
      
      @Override
      public void stop(){ } 
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey) { return validKeys.get().contains(messageKey); }
      
      public FixedInbound set(Object... keys) { validKeys.set(new HashSet<Object>(Arrays.asList(keys))); return this; }
      
      public FixedInbound clear() { validKeys.set(new HashSet<Object>(Arrays.asList())); return this; }
   }
   
   static class FixedKeySource implements KeySource<String>
   {
      AtomicReference<List<String>> keys = new AtomicReference<List<String>>(new ArrayList<String>());
      
      @Override
      public Iterable<String> getAllPossibleKeys() { return keys.get(); }
      
      public FixedKeySource set(String[]...keys)
      {
         List<String> newKeys = new ArrayList<String>();
         for (String[] keyset : keys)
            newKeys.addAll(Arrays.asList(keyset));
         this.keys.set(newKeys);
         return this;
      }
   }
   
   @Test
   public void testKeyspaceExpandThenContraction() throws Throwable
   {
      final String[] keys = { "foo1", "foo2" };
      
      // set an inbound strategy that provides a known value
      FixedInbound inbound = new FixedInbound().set((Object[])keys);
      FixedKeySource keysource = new FixedKeySource().set(keys);
      
      // set a keysource with 2 keys
      container.setKeySource(keysource);
      
      container.keyspaceResponsibilityChanged(inbound, false, true);
      
      TestProcessor prototype = context.getBean(TestProcessor.class);
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, prototype.cloneCount, new TestUtils.Condition<AtomicInteger>() 
            { @Override public boolean conditionMet(AtomicInteger o) { return o.intValue() == 2; } }));
      
      Thread.sleep(10);
      assertEquals(2,prototype.cloneCount.intValue());
      
      inbound.clear(); // force the Inbound to deny that any Mp should run here.
      container.keyspaceResponsibilityChanged(inbound, true, false);
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, container, new TestUtils.Condition<MpContainer>() 
            { @Override public boolean conditionMet(MpContainer o) { return o.getInstances().size() == 0; } }));
      
      Thread.sleep(10);
      assertEquals(0,container.getInstances().size());
   }
   
   @Test
   public void testKeyspaceExpandAndContraction() throws Throwable
   {
      final String[] keys = { "foo1", "foo2" };
      final String[] keysShard2 = { "foo3", "foo4" };
      
      // set an inbound strategy that provides a known value
      FixedInbound inbound = new FixedInbound().set((Object[])keys);
      FixedKeySource keysource = new FixedKeySource().set(keys, keysShard2);
      
      // set a keysource with 2 keys
      container.setKeySource(keysource);
      
      container.keyspaceResponsibilityChanged(inbound, false, true);
      
      TestProcessor prototype = context.getBean(TestProcessor.class);
      assertTrue(TestUtils.poll(baseTimeoutMillis, prototype.cloneCount, new TestUtils.Condition<AtomicInteger>() 
            { @Override public boolean conditionMet(AtomicInteger o) { return o.intValue() == 2; } }));
      
      Thread.sleep(10);
      assertEquals(2,prototype.cloneCount.intValue());
      
      inbound.set((Object[])keysShard2); // force the Inbound to deny that any Mp should run here.
      container.keyspaceResponsibilityChanged(inbound, true, true);
      
      // check to make sure the total clone count is now 4 since two new Mps should be there
      assertTrue(TestUtils.poll(baseTimeoutMillis, prototype.cloneCount, new TestUtils.Condition<AtomicInteger>() 
            { @Override public boolean conditionMet(AtomicInteger o) { return o.intValue() == 4; } }));
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, container, new TestUtils.Condition<MpContainer>() 
            { @Override public boolean conditionMet(MpContainer o) { return o.getInstances().size() == 2; } }));
      
      Thread.sleep(10);
      assertEquals(2,container.getInstances().size());
   }
   
   @Test
   public void testOverlappingKeyspaceContraction() throws Throwable
   {
      // need to get hold of the block passivate latch and set it to 1
      TestProcessor prototype = context.getBean(TestProcessor.class);
      CountDownLatch blockPassivate = new CountDownLatch(1);
      prototype.blockPassivate = blockPassivate;
      
      final String[] keys = { "foo1", "foo2" };
      
      // set an inbound strategy that provides a known value
      final FixedInbound inbound = new FixedInbound().set((Object[])keys);
      FixedKeySource keysource = new FixedKeySource().set(keys);
      
      // set a keysource with 2 keys
      container.setKeySource(keysource);
      
      container.keyspaceResponsibilityChanged(inbound, false, true);
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, prototype.cloneCount, new TestUtils.Condition<AtomicInteger>() 
            { @Override public boolean conditionMet(AtomicInteger o) { return o.intValue() == 2; } }));
      
      Thread.sleep(10);
      assertEquals(2,prototype.cloneCount.intValue());
      
      // ok ... we have a container with 2 mps.
      // now we're going to passivate but the passivate will block
      
      inbound.clear(); // force the Inbound to deny that any Mp should run here.
      container.keyspaceResponsibilityChanged(inbound, true, false);
      
      // the Mp deletion should be hung in passivate on the frist one ... this test will break if eviction
      //  becomes concurrent since both Mps will be blocked and released at the same time. We want the 
      //  second.
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, prototype.passivateCount, new TestUtils.Condition<AtomicLong>() 
            { @Override public boolean conditionMet(AtomicLong o) { return o.intValue() == 1; } }));
      Thread.sleep(10);
      assertEquals(1,prototype.passivateCount.get());
      
      // kick force a redo of the keyspaceResponsibilityChange. This will hang without the latch so
      // we kick it off in the background. Here we are going to reset the inbound so that it rescues the
      // other Mp
      inbound.set((Object[])keys);
      final AtomicBoolean isRunningKSChange = new AtomicBoolean(false);
      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               isRunningKSChange.set(true);
               container.keyspaceResponsibilityChanged(inbound, true, false);
            }
            finally
            {
               isRunningKSChange.set(false);
            }
         }
      });
      t.setDaemon(true);
      t.start();
      
      // let's make sure the keyspaceChanged call is hanging
      assertTrue(TestUtils.poll(baseTimeoutMillis, isRunningKSChange, new TestUtils.Condition<AtomicBoolean>() 
            { @Override public boolean conditionMet(AtomicBoolean o) { return o.get(); } }));
      Thread.sleep(10);
      assertTrue(isRunningKSChange.get());

      // the above will block unless the latch is released
      blockPassivate.countDown(); // this lets them all go
      
      // this should result in preservation of the other Mps since we preempted the keyspace deletion
      assertTrue(TestUtils.poll(baseTimeoutMillis, container, new TestUtils.Condition<MpContainer>() 
            { @Override public boolean conditionMet(MpContainer o) { return o.getInstances().size() == 1; } }));
      Thread.sleep(10);
      assertEquals(1,container.getInstances().size());
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, isRunningKSChange, new TestUtils.Condition<AtomicBoolean>() 
            { @Override public boolean conditionMet(AtomicBoolean o) { return !o.get(); } }));
   }

}
