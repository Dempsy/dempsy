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
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;

public class BlockingQueueTest
{
   private ClassPathXmlApplicationContext ctx;
   private Sender sender;
   private SenderFactory senderFactory;
   private BlockingQueueAdaptor destinationFactory;
   private MyPojo pojo;
   
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
   
   public void setUp(String applicationContextFilename) throws Exception
   {

      ctx = new ClassPathXmlApplicationContext(applicationContextFilename, getClass());
      ctx.registerShutdownHook();        
      
      Sender lsender = (Sender)ctx.getBean("sender");
      sender = lsender;
      
      pojo = (MyPojo)ctx.getBean("testPojo");
   }

   public void setUp2(String applicationContextFilename) throws Exception
   {
      ctx = new ClassPathXmlApplicationContext(applicationContextFilename, getClass());
      ctx.registerShutdownHook();        
      
      SenderFactory lsender = (SenderFactory)ctx.getBean("senderFactory");
      senderFactory = lsender;
      destinationFactory = (BlockingQueueAdaptor)ctx.getBean("adaptor");
      
      pojo = (MyPojo)ctx.getBean("testPojo");
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
      sender = null;
      senderFactory = null;
      destinationFactory = null;
   }
   
   private static MessageBufferOutput makeMessageBuffer(byte[] data)
   {
      final MessageBufferOutput buffer = new MessageBufferOutput(0){};
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
      }
      finally
      {
         if (ctx != null)
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
      }
      finally
      {
         if (ctx != null)
            tearDown();
      }
   }
}
