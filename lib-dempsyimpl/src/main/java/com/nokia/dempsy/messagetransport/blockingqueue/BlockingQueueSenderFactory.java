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

import java.util.HashMap;
import java.util.Map;

import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.monitoring.StatsCollector;

public class BlockingQueueSenderFactory implements SenderFactory
{
   private Map<Destination,BlockingQueueSender> senders = new HashMap<Destination,BlockingQueueSender>();
   private StatsCollector statsCollector;
   
   public BlockingQueueSenderFactory(StatsCollector statsCollector) { this.statsCollector = statsCollector; }
   
   @Override
   public synchronized Sender getSender(Destination destination) throws MessageTransportException
   {
      BlockingQueueSender blockingQueueSender = senders.get(destination);
      if (blockingQueueSender == null)
      {
         blockingQueueSender = new BlockingQueueSender(statsCollector);
         blockingQueueSender.setQueue(((BlockingQueueDestination)destination).queue);
         senders.put(destination, blockingQueueSender);
      }
      
      return blockingQueueSender;
   }
   
   @Override
   public synchronized void stopDestination(Destination destination)
   {
      BlockingQueueSender blockingQueueSender = senders.get(destination);
      if (blockingQueueSender != null)
         blockingQueueSender.shuttingDown();
   }
   
   @Override
   public void shutdown()
   {
      for (BlockingQueueSender sender : senders.values())
         sender.shuttingDown();
   }

   private static class ConstructableMessageBufferOutput extends MessageBufferOutput { public ConstructableMessageBufferOutput() { } }
   
   @Override public MessageBufferOutput prepareMessage() { return new ConstructableMessageBufferOutput(); }

}
