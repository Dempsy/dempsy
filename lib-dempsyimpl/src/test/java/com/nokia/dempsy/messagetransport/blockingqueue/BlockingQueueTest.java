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

package com.nokia.dempsy.messagetransport.blockingqueue;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;

public class BlockingQueueTest
{
   private ClassPathXmlApplicationContext ctx;
   private Sender sender;
   private SenderFactory senderFactory;
   private BlockingQueueAdaptor destinationFactory;
   private MyPojo pojo;
   private MyOverflowHandler overflowHandler;
   
   public static class MyPojo
   {
      public BlockingQueue<MessageBufferInput> receiver;
      
      public void setQueue(BlockingQueue<MessageBufferInput> receiver)
      {
         this.receiver = receiver;
      }
      
      public MessageBufferInput getMessage() throws Exception
      {
         return receiver.take();
      }
   }
   
   public static class MyOverflowHandler implements OverflowHandler
   {
      public int overflowCalled = 0;
      
      @Override
      public synchronized void overflow(MessageBufferInput messageBytes)
      {
         overflowCalled++;
         notifyAll();
      }
      
   }
   
   public void setUp(String applicationContextFilename) throws Exception
   {

      ctx = new ClassPathXmlApplicationContext(applicationContextFilename, getClass());
      ctx.registerShutdownHook();        
      
      Sender lsender = (Sender)ctx.getBean("sender");
      sender = lsender;
      
      pojo = (MyPojo)ctx.getBean("testPojo");
      overflowHandler = (MyOverflowHandler)ctx.getBean("testOverflowHandler");
   }

   public void setUp2(String applicationContextFilename) throws Exception
   {
      ctx = new ClassPathXmlApplicationContext(applicationContextFilename, getClass());
      ctx.registerShutdownHook();        
      
      SenderFactory lsender = (SenderFactory)ctx.getBean("senderFactory");
      senderFactory = lsender;
      destinationFactory = (BlockingQueueAdaptor)ctx.getBean("adaptor");
      
      pojo = (MyPojo)ctx.getBean("testPojo");
      overflowHandler = (MyOverflowHandler)ctx.getBean("testOverflowHandler");
   }


   /**
    * @throws java.lang.Exception
    */
   public void tearDown() throws Exception
   {
      if (ctx != null)
      {
         ctx.close();
         ctx.destroy();
         ctx = null;
      }
      pojo = null;
      overflowHandler = null;
      sender = null;
      senderFactory = null;
      destinationFactory = null;
   }
   
   private static MessageBufferOutput makeMessageBuffer(byte[] data)
   {
      final MessageBufferOutput buffer = new MessageBufferOutput(0);
      buffer.replace(data);
      buffer.setPosition(data.length);
      return buffer;
   }

   /*
    * Test basic functionality for the BlockingQueue implementation
    * of Message Transport.  Verify that messages sent to the Sender
    * arrive at the receiver via handleMessage.
    */
   @Test
   public void testBlockingQueue() throws Throwable
   {
      try
      {
         setUp("/blockingqueueTestAppContext.xml");
         sender.send(makeMessageBuffer("Hello".getBytes()));
         String message = new String(  pojo.getMessage().getBuffer() );
         assertEquals("Hello", message);
         assertEquals(0, overflowHandler.overflowCalled);
      }
      finally
      {
         if (ctx != null)
            tearDown();
      }
   }

   /**
    * Test a non-blocking Transport without an overflow handler
    * on the transport.  Should call overflow handler once
    * queue is full.
    * @throws Throwable
    */
   @Test
   public void testNonBlockingQueueOverflow() throws Throwable
   {
      try
      {
         setUp("/overflowTestAppContext.xml");
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(0,overflowHandler.overflowCalled);
         
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello again".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(0,overflowHandler.overflowCalled);
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello I must be going.".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(0,overflowHandler.overflowCalled);
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello I must be going again.".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(1,overflowHandler.overflowCalled);
         sender.send(makeMessageBuffer("Hello again I must be going again.".getBytes()));
      }
      finally
      {
         tearDown();
      }
   }

   /**
    * Test overflow for a blocking Transport around a queue with depth one.
    * While the transport will not call the, and does not even have a , overflow handler,
    * every message will call the overflow handler on the receiver since the queue is
    * always full.
    * 
    * @throws Throwable
    */
   @Test
   public void testBlockingQueueOverflow() throws Throwable
   {
      try
      {
         setUp("/overflowTest2AppContext.xml");
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(0,overflowHandler.overflowCalled);
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello again".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(1,overflowHandler.overflowCalled);
         synchronized(overflowHandler) /// avoid a race condition
         {
            sender.send(makeMessageBuffer("Hello I must be going.".getBytes()));
            overflowHandler.wait(500);
         }
         assertEquals(2,overflowHandler.overflowCalled);
      }
      finally
      {
         tearDown();
      }
   }
   
   @Test
   public void testBlockingQueueUsingDestination() throws Exception
   {
      try
      {
         setUp2("/blockingqueueTest2AppContext.xml");
         Destination destination = destinationFactory.getDestination();
         Sender lsender = senderFactory.getSender(destination);
         lsender.send(makeMessageBuffer("Hello".getBytes()));
         String message = new String( pojo.getMessage().getBuffer() );
         assertEquals("Hello", message);
         assertEquals(0, overflowHandler.overflowCalled);
      }
      finally
      {
         if (ctx != null)
            tearDown();
      }
   }
}
