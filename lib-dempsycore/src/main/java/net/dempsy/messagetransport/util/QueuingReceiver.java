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

package net.dempsy.messagetransport.util;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.messagetransport.Listener;
import net.dempsy.messagetransport.MessageTransportException;

/**
 * <p>This is a concrete class that can be used by a MessageTransport client that 
 * would prefer to pull messages rather than have them pushed them. It 
 * implements the MessageTransportListener and can be wired into an implementation
 * Adaptor and also wired into the client that wants to pull messages.</p>
 * 
 * <p>It needs to be initialized with the BlockingQueue that will be used to 
 * pull from.</p>
 * 
 * <p>It can optionally be wired to an MessageOverflowHandler that will be
 * called when the blocking queue is at capacity. When the BlockingQueue
 * is at capacity and there is no MessageOverflowHandler the MessageTransportQueuingReceiver
 * will simply block until room becomes available.</p>
 *
 */
public class QueuingReceiver implements Listener
{
   private static Logger logger = LoggerFactory.getLogger(QueuingReceiver.class);
   private BlockingQueue<byte[]> queue;

   /**
    * The queue that the MessageTransportQueuingReceiver puts messages on and a client
    * can pull messages from.
    * 
    * @param queue is the BlockingQueue to be used.
    */
   public void setQueue(BlockingQueue<byte[]> queue)
   {
      this.queue = queue;
   }
   
   /**
    * The client is expected to get the BlockingQueue from the MessageTransportQueuingReceiver
    * and use it directly.
    * 
    * @return the BlockingQueue that messages are being pushed to, and for the client
    * to pull from.
    */
   public BlockingQueue<byte[]> getQueue()
   {
      return this.queue;
   }
   
   /**
    * This implements the MessageTransportListener requirement. It will push the
    * message onto the BlockingQueue. If the queue is full and there is a 
    * MessageOverflowHandler supplied then the MessageOverflowHandler will be 
    * invoked to handle the message. Otherwise it will block until there is 
    * room in the queue.
    */
   @Override
   public boolean onMessage(byte[] messageBytes, boolean failFast) throws MessageTransportException
   {
      if (failFast)
         return queue.offer(messageBytes);
      else
      {
         try { queue.put(messageBytes); return true; }
         catch(InterruptedException ie)
         {
            logger.warn("Pushing the message to the queue was interrupted.");
            return false;
         }
      }
   }
   
   /**
    * This implements the MessageTransportListener requirement. It does nothing.
    */
   @Override
   public void shuttingDown() {  }
   
}
