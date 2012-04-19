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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.container.mocks.MockInputMessage;
import com.nokia.dempsy.container.mocks.MockOutputMessage;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.coda.StatsCollectorCoda;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;
import com.nokia.dempsy.serialization.java.JavaSerializer;

/**
 * Test load handling / sheding in the MP container.
 * This is probably involved enough to merit not mixing into
 * the existing MPContainer test cases.
 */
public class TestMpContainerLoadHandling 
{
   private void checkStat(StatsCollector stat)
   {
      assertEquals(stat.getDispatchedMessageCount(), 
            stat.getMessageFailedCount() + stat.getProcessedMessageCount() + 
            stat.getDiscardedMessageCount() + stat.getInFlightMessageCount());
   }

   private static int NTHREADS = 2;
   private static Logger logger = LoggerFactory.getLogger(TestMpContainerLoadHandling.class);
   
   private MpContainer container;
   private StatsCollector stats;
   private MockDispatcher dispatcher;
   private CountDownLatch startLatch; // when set, the TestMessageProcessor waits to begin processing
   private CountDownLatch finishLatch; // counts down when any instance of TestMessageProcessor COMPLETES handling a message
   private CountDownLatch imIn; // this counts how many TestMessageProcessor message handlers, OR output calls have been entered
   private CountDownLatch finishOutputLatch;
   private volatile boolean forceOutputException = false;
   
   private int sequence = 0;
   
   @Before
   public void setUp() throws Exception 
   {
      ClusterId cid = new ClusterId("TestMpContainerLoadHandling", "test" + sequence++);
      dispatcher = new MockDispatcher();

      stats = new StatsCollectorCoda(cid);
      JavaSerializer<Object> serializer = new JavaSerializer<Object>();

      container = new MpContainer(cid);
      container.setDispatcher(dispatcher);
      container.setStatCollector(stats);
      container.setSerializer(serializer);
      container.setPrototype(new TestMessageProcessor());
      
      forceOutputException = false;
   }
   
   @After
   public void tearDown() throws Exception
   {
      stats.stop();
   }
   
   public static boolean failed = false;
   
   public static class SendMessageThread implements Runnable
   {
      MpContainer mpc;
      Object message;
      boolean block;
      
      static CountDownLatch latch = new CountDownLatch(0);
      
      public SendMessageThread(MpContainer mpc, Object message, boolean block)
      {
         this.mpc = mpc;
         this.message = message;
         this.block = block;
      }
      
      @Override
      public void run()
      {
         try
         {
            Serializer<Object> serializer = new JavaSerializer<Object>();
            byte[] data = serializer.serialize(message);
            mpc.onMessage(data,!block); // onmessage is "failfast" which is !block
         }
         catch(SerializationException e)
         {
            failed = true;
            System.out.println("FAILED!");
            e.printStackTrace();
            throw new RuntimeException(e);
         }
         finally
         {
            latch.countDown();
         }
      }
   }
   
   /*
    * Utility methods for tests
    */
   private void sendMessage(MpContainer mpc, Object message, boolean block) throws Exception
   {
      new Thread(new SendMessageThread(mpc, message, block)).start();
   }
   
   /*
    * Test Infastructure
    */
   class MockDispatcher implements Dispatcher
   {
      public CountDownLatch latch = null;
      
      public List<Object> messages = Collections.synchronizedList(new ArrayList<Object>());
      
      @Override
      public void dispatch(Object message)
      {
         // TODO remove this it's debugging
         assertNotNull(message);
         messages.add(message);
         if (latch != null)
            latch.countDown();
      }
   }
   
   @MessageProcessor
   public class TestMessageProcessor implements Cloneable
   {
      int messageCount = 0;
      String key;
   
      @Override
      public TestMessageProcessor clone() throws CloneNotSupportedException
      {
         return (TestMessageProcessor)super.clone();
      }
      
      @MessageHandler
      public MockOutputMessage handleMessage(MockInputMessage msg) throws InterruptedException
      {
         imIn.countDown();
         startLatch.await();
         logger.trace("handling key " + msg.getKey() + " count is " + messageCount);
         key = msg.getKey();
         messageCount++;
         msg.setProcessed(true);
         MockOutputMessage out = new MockOutputMessage(msg.getKey());
         finishLatch.countDown();
         return out;
      }
      
      @Output
      public MockOutputMessage doOutput() throws InterruptedException 
      {
         logger.trace("handling output message for mp with key " + key);
         imIn.countDown();
//         System.out.println("In Output with countdown:" + imIn);
         MockOutputMessage out = new MockOutputMessage(key, "output");
         if (finishOutputLatch != null)
            finishOutputLatch.countDown();
         
         if (forceOutputException)
            throw new RuntimeException("Forced Exception!");
         
         return out;
      }
   }

   /*
    * Actual Unit Tests
    */
   
   /**
    * Test the simple case of messages arriving slowly and always
    * having a thread available.
    */
   @Test
   public void testSimpleProcessingWithoutQueuing() throws Exception
   {
      // Send NTHREADS Messages and wait for them to come out, then repeat
      for (int i = 0; i < 3; i++)
      {
         startLatch = new CountDownLatch(1);
         finishLatch = new CountDownLatch(NTHREADS);
         imIn = new CountDownLatch(NTHREADS);
         dispatcher.latch = new CountDownLatch(NTHREADS);
         
         ArrayList<MockInputMessage> in = new ArrayList<MockInputMessage>();
         for (int j = 0; j < NTHREADS; j++)
            in.add(new MockInputMessage("key" + j));
      
         SendMessageThread.latch = new CountDownLatch(in.size());
         for (MockInputMessage m: in)
            sendMessage(container, m, false);
         
         logger.trace("releasing workers");
         startLatch.countDown();
         logger.trace("should be running");

         // after sends are allowed to proceed
         assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));
      
         assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
         while (stats.getInFlightMessageCount() > 0)
            Thread.yield(); // give worker threads a chance to finish returning post latch
      
         assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
         assertEquals(NTHREADS * (i + 1), dispatcher.messages.size());
         assertEquals(NTHREADS * (i + 1), stats.getProcessedMessageCount());
      }
      
      checkStat(stats);
   }

   /**
    * Test the case where messages arrive faster than they are processed
    * but the queue never exceeds the Thread Pool Size * 2, so no messages
    * are discarded.
    */
   @Test
   public void testMessagesCanQueueWithinLimits() throws Exception
   {
      // Since the discard threshold is 2 * threads, send 3 * NTHREADS Messages,
      // the 2*threads that are queued, plus the ntheads that are stalled
      // in processing.  Then wait for them to come out, none should be discarded
      // Repeat the test to cover edge conditions that work the first time magically
      for (int i = 0; i < 3; i++)
      {
         logger.trace("starting loop " + i);
         startLatch = new CountDownLatch(1);
         finishLatch = new CountDownLatch(3 * NTHREADS);
         imIn = new CountDownLatch(3 * NTHREADS);
         dispatcher.latch = new CountDownLatch(3 * NTHREADS);

         ArrayList<MockInputMessage> in = new ArrayList<MockInputMessage>();
         for (int j = 0; j < (3 * NTHREADS); j++)
            in.add(new MockInputMessage("key" + j));
      
         SendMessageThread.latch = new CountDownLatch(in.size());
         for (MockInputMessage m: in)
         {
            sendMessage(container, m,false);
            Thread.sleep(50); // let the exector so it can put the first two on threads
            assertEquals(0, stats.getDiscardedMessageCount());
            Thread.yield(); // allow the newly running thread to move on.
         }
         startLatch.countDown();
         assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));
      
         assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
         while (stats.getInFlightMessageCount() > 0)
            Thread.yield(); // Give worker threads a chance to finish their returns
      
         assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
         assertEquals(3 * NTHREADS * (i + 1), dispatcher.messages.size());
         assertEquals(3 * NTHREADS * (i + 1), stats.getProcessedMessageCount());
         checkStat(stats);
      }
   }
   
   @Test
   public void testExcessLoadIsDiscarded() throws Exception
   {
      
      startLatch = new CountDownLatch(1);
      finishLatch = new CountDownLatch(3 * NTHREADS);
      imIn = new CountDownLatch(3 * NTHREADS);
      dispatcher.latch = new CountDownLatch(3 * NTHREADS);
      ArrayList<MockInputMessage> in = new ArrayList<MockInputMessage>();
      for (int j = 0; j < (3 * NTHREADS); j++)
         in.add(new MockInputMessage("key" + j));
      
      // load up the queue with messages, none get discarded
      SendMessageThread.latch = new CountDownLatch(in.size());
      for (MockInputMessage m: in)
      {
         sendMessage(container, m,false);
         assertEquals(0, stats.getDiscardedMessageCount());
      }
      
      // Add another message.  Since processing is wating on the start latch,
      /// it shoulld be discarded
      assertTrue(imIn.await(2, TimeUnit.SECONDS)); // this means they're all in
      // need to directly dispatch it to avoid a race condition
      container.dispatch(new MockInputMessage("key" + 0), false);
      assertEquals(1, stats.getDiscardedMessageCount());
      container.dispatch(new MockInputMessage("key" + 1), false);
      assertEquals(2, stats.getDiscardedMessageCount());
      
      checkStat(stats);
      startLatch.countDown();
      checkStat(stats);

      // after sends are allowed to proceed
      assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));

      assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
      while(stats.getInFlightMessageCount() > 0){
         Thread.yield();
      }
      
      assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
      assertEquals(3 * NTHREADS, dispatcher.messages.size());
      assertEquals(3 * NTHREADS, stats.getProcessedMessageCount());
      dispatcher.latch = new CountDownLatch(3 * NTHREADS); // reset the latch

      // Since the queue is drained, we should be able to drop a few more
      // on and they should just go
      finishLatch = new CountDownLatch(3 * NTHREADS);
      SendMessageThread.latch = new CountDownLatch(in.size());
      for (MockInputMessage m: in)
      {
         sendMessage(container, m, false);
         Thread.sleep(50);
         assertEquals(2, stats.getDiscardedMessageCount());
      }
      assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));

      assertTrue("Timeout waiting for MPs", finishLatch.await(2, TimeUnit.SECONDS));
      while (stats.getInFlightMessageCount() > 0)
         Thread.yield();

      assertTrue("Timeout waiting for MPs", dispatcher.latch.await(2, TimeUnit.SECONDS));
      assertEquals(2 * 3 * NTHREADS, dispatcher.messages.size());
      assertEquals(2 * 3 * NTHREADS, stats.getProcessedMessageCount());
      checkStat(stats);
   }

   @Test
   public void testOutputOperationsNotDiscarded() throws Exception{

      // prime the container with MP instances, by processing 4 * NTHREADS messages
      startLatch = new CountDownLatch(0); // do not hold up the starting of processing
      finishLatch = new CountDownLatch(NTHREADS * 4); // we expect this many messages to be handled.
      imIn = new CountDownLatch(4 * NTHREADS); // this is how many times the message processor handler has been entered
      dispatcher.latch = new CountDownLatch(4 * NTHREADS); // this counts down whenever a message is dispatched
      SendMessageThread.latch = new CountDownLatch(4 * NTHREADS); // when spawning a thread to send a message into an MpContainer, this counts down once the message sending is complete

      // invoke NTHREADS * 4 discrete messages each of which should cause an Mp to be created
      for (int i = 0; i < (NTHREADS * 4); i++) 
         sendMessage(container, new MockInputMessage("key" + i),false);
      
      // wait for all of the messages to have been sent.
      assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));
      
      // wait for all of the messages to have been handled by the new Mps
      assertTrue("Timeout on initial messages", finishLatch.await(4, TimeUnit.SECONDS));
      
      // the above can be triggered while still within the message processor handler so we wait
      //  for the stats collector to have registered the processed messages.
      Thread.yield(); // give the threads a chance to return after updating the latch
      for (long endTime = System.currentTimeMillis() + 10000; stats.getInFlightMessageCount() > 0 && endTime > System.currentTimeMillis();)
         Thread.sleep(1); // give worker threads a chance to finish returning post latch

      // with the startLatch set to 0 these should all complete.
      assertEquals(0,stats.getInFlightMessageCount());
      assertEquals(4 * NTHREADS, stats.getProcessedMessageCount());

      startLatch = new CountDownLatch(1); // now we are going to hold the Mps inside the message handler
      
      // we are going to send 2 messages (per thread) 
      finishLatch = new CountDownLatch(2 * NTHREADS); 
      // ... and wait for the 2 message (per thread) to be in process (this startLatch will hold these from exiting)
      imIn = new CountDownLatch(2 * NTHREADS); // (2 messages + 4 output) * nthreads

      // send 2 message (per thread) which will be held up by the startLatch
      for (int i = 0; i < (2 * NTHREADS); i++)
      {
         sendMessage(container, new MockInputMessage("key" + i),false);
         assertEquals(0, stats.getDiscardedMessageCount());
      }
      
      // there ought to be 2 * NTHREADS inflight ops since the startLatch isn't triggered
      assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS));
      checkStat(stats);
      assertEquals(2 * NTHREADS, stats.getInFlightMessageCount());
      
      // with those held up in message handling they wont have the output invoked. So when we invoke output we need to count
      // how many get through while the message processors are being held up. That means there should be 2 * NTHREAD held up
      // in message handling and 2 * NTHREAD more that can execute the output (since the total is 4 * NTHREAD).
      finishOutputLatch = new CountDownLatch(2 * NTHREADS); // we should see 2 output

      long totalProcessedCount = stats.getProcessedMessageCount();
      // Generate an output message
      imIn = new CountDownLatch(2 * NTHREADS);
      
      // kick off the output pass in another thread
      new Thread(new Runnable() {
         @Override
         public void run() { container.outputPass(); }
      }).start();
      
      // 2 * NTHREADS are at startLatch while there are 4 * NTHREADS total MPs. 
      //   Thererfore two outputs should have executed and two more are thrashing now. 
      assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS)); // wait for the 2 * NTHREADS output calls to be entered
      assertTrue("Timeout on initial messages", finishOutputLatch.await(4 * NTHREADS, TimeUnit.SECONDS)); // wait for those two to actually finish output processing
      
      // there's a race condition between finishing the output and the output call being registered so we need to wait
      for (long endTime = System.currentTimeMillis() + (NTHREADS * 4000); endTime > System.currentTimeMillis() && 
            ((2L * NTHREADS) + totalProcessedCount != stats.getProcessedMessageCount());)
         Thread.sleep(1);

      assertEquals((2L * NTHREADS) + totalProcessedCount,stats.getProcessedMessageCount());

      assertEquals(0, stats.getDiscardedMessageCount());  // no discarded messages
      checkStat(stats);

      imIn = new CountDownLatch(2 * NTHREADS); // give me a place to wait for the remaining outputs.
      startLatch.countDown(); // let the 2 MPs that are waiting run.
      assertTrue("Timeout waiting for MPs", finishLatch.await(6, TimeUnit.SECONDS));
      assertTrue("Timeout waiting for MP outputs to finish", imIn.await(6, TimeUnit.SECONDS));
      
      while (stats.getInFlightMessageCount() > 0)
         Thread.yield();
      
      assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));

      // all output messages were processed
      int outMessages = 0;
      for (Object o: ((MockDispatcher)dispatcher).messages)
      {
         MockOutputMessage m = (MockOutputMessage)o;
         if (m == null)
            fail("wtf!?");
         if ("output".equals(m.getType()))
            outMessages++;
      }
      assertEquals(4 * NTHREADS, outMessages);
      checkStat(stats);

   }

   @Test
   public void testOutputOperationsAloneDoNotCauseDiscard() throws Exception{
      // prime the container with MP instances, by processing 4 * NTHREADS messages
      startLatch = new CountDownLatch(0);
      finishLatch = new CountDownLatch(NTHREADS * 4);
      imIn = new CountDownLatch(0);
      dispatcher.latch = new CountDownLatch(10 * NTHREADS);
      
      SendMessageThread.latch = new CountDownLatch(NTHREADS * 4);
      for (int i = 0; i < (NTHREADS * 4); i++)
         sendMessage(container, new MockInputMessage("key" + i),false);
      assertTrue("Timeout on initial messages", finishLatch.await(4, TimeUnit.SECONDS));
      Thread.yield();  // cover any deltas between mp decrementing latch and returning
      assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));
      checkStat(stats);

      assertEquals(NTHREADS * 4, stats.getProcessedMessageCount());

      startLatch = new CountDownLatch(1);
      finishLatch = new CountDownLatch(6 * NTHREADS); // (4 output + 2 msg) * nthreads
      // Generate an output message but force a failure.
      forceOutputException = true;
      container.outputPass(); // this should be ok inline.
      forceOutputException = false;
      assertEquals(0, stats.getDiscardedMessageCount());  // no discarded messages
      assertEquals(NTHREADS * 4, stats.getMessageFailedCount()); // all output messages should have failed.
      
      // run it again without the null pointer exception
      finishOutputLatch = new CountDownLatch(NTHREADS * 4);
      container.outputPass(); // this should be ok inline.
      assertEquals(0, stats.getDiscardedMessageCount());  // no discarded messages
      assertEquals(NTHREADS * 4, stats.getMessageFailedCount()); // all output messages should have failed.
      assertEquals(2 * (NTHREADS * 4), stats.getProcessedMessageCount());
      checkStat(stats);
      
      imIn = new CountDownLatch(2 * NTHREADS);
      finishLatch = new CountDownLatch(2 * NTHREADS);
      SendMessageThread.latch = new CountDownLatch(NTHREADS * 2);
      // put 2 * nthreads messages on the queue, expect no discards
      for (int i = 0; i < (2 * NTHREADS); i++)
      {
         sendMessage(container, new MockInputMessage("key" + i),false);
         assertEquals(0, stats.getDiscardedMessageCount());
      }

      assertTrue("Timeout on initial messages", imIn.await(4, TimeUnit.SECONDS));
      assertEquals(0, stats.getDiscardedMessageCount());
      checkStat(stats);

      // let things run
      startLatch.countDown();
      assertTrue("Timeout waiting on MPs", finishLatch.await(6, TimeUnit.SECONDS));

      // after sends are allowed to proceed
      assertTrue("Timeout waiting on message to be sent",SendMessageThread.latch.await(2, TimeUnit.SECONDS));

      assertTrue("Timeout waiting for MP sends", dispatcher.latch.await(2, TimeUnit.SECONDS));
      assertEquals(0, stats.getDiscardedMessageCount());
      assertEquals(10 * NTHREADS, ((MockDispatcher)dispatcher).messages.size());

   }   
}
