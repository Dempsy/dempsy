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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Sender;

/**
 * <p>The Message transport default library comes with this BlockingQueue implementation.</p>
 * 
 * <p>This class represents the Sender. You need to initialize it with the BlockingQueue to use.</p>
 */
public class BlockingQueueSender implements Sender
{
   protected BlockingQueue<byte[]> queue;
   protected OverflowHandler overflowHandler;
   
   protected AtomicBoolean shutdown = new AtomicBoolean(false);

   protected boolean blocking = true;
   
   /**
    * This satisfies the requirement of a MessageTransportSender. It will stop the thread
    * that's handling the receive side of the BlockingQueue. @PreDestroy is set so that
    * containers that handle standard bean lifecycle annotations will automatically issue 
    * a 'shutdown' when the container is stopped.
    */
   public void shuttingDown()
   {
      shutdown.set(true);
   }

   /**
    * Send a message into the BlockingQueueAdaptor.
    */
   @Override
   public void send(byte[] messageBytes) throws MessageTransportException
   {
      if (shutdown.get())
         throw new MessageTransportException("send called on shutdown queue.");
      
      if (blocking)
      {
         while (true)
         {
            try { queue.put(messageBytes);  break; }
            catch (InterruptedException ie)
            {
               if (shutdown.get())
                  throw new MessageTransportException("Shutting down durring send.");
            }
         }
      }
      else if (! queue.offer(messageBytes))
      {
         if (overflowHandler != null)
            overflowHandler.overflow(messageBytes);
         else
            throw new MessageTransportException("Failed to queue message due to capacity.");
      }
   }
   
   /**
    * This sets the BlockingQueue implementation to be used in the transport. 
    * It must be set prior to starting.
    * 
    * @param queue is the BlockingQueue implementation to use.
    */
   public void setQueue(BlockingQueue<byte[]> queue){ this.queue = queue; }
   
   /**
    * This can be set optionally and will be used on the send side. 
    * @param overflowHandler
    */
   public void setOverflowHandler(OverflowHandler overflowHandler){ this.overflowHandler = overflowHandler; }
   
   /**
    * <p>blocking is 'true' be default (after all this is a <b>Blocking</b>Queue 
    * implementation).</p>
    * 
    * <p>If blocking is true then the transport will use the blocking calls on the
    * BlockingQueue when sending, and therefore the OverflowHandler, whether set or not,
    * will never get called.</p?
    * 
    * <p>When blocking is 'false' a send to a full queue will result in:
    * <li> if the MessageOverflowHandler is set, it will pass the message to it.</li>
    * <li> otherwise it will throw a MessageTransportException. </li>
    * </p>
    * 
    * @param blocking is whether or not to set this queue to blocking. It can be
    * changed after a queue is started but there is no synchronization around the
    * checking in the send method.
    */
   public void setBlocking(boolean blocking) { this.blocking = blocking; }
   
}
