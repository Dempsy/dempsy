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

package net.dempsy.messagetransport.blockingqueue;

import java.util.HashMap;
import java.util.Map;

import net.dempsy.messagetransport.Destination;
import net.dempsy.messagetransport.MessageTransportException;
import net.dempsy.messagetransport.OverflowHandler;
import net.dempsy.messagetransport.Sender;
import net.dempsy.messagetransport.SenderFactory;
import net.dempsy.monitoring.StatsCollector;

public class BlockingQueueSenderFactory implements SenderFactory
{
   private Map<Destination,BlockingQueueSender> senders = new HashMap<Destination,BlockingQueueSender>();
   private OverflowHandler overflowHandler = null;
   private StatsCollector statsCollector;
   
   public BlockingQueueSenderFactory(StatsCollector statsCollector) { this.statsCollector = statsCollector; }
   
   public void setOverflowHandler(OverflowHandler overflowHandler) { this.overflowHandler = overflowHandler; }
   
   @Override
   public synchronized Sender getSender(Destination destination) throws MessageTransportException
   {
      BlockingQueueSender blockingQueueSender = senders.get(destination);
      if (blockingQueueSender == null)
      {
         blockingQueueSender = new BlockingQueueSender(statsCollector);
         blockingQueueSender.setQueue(((BlockingQueueDestination)destination).queue);
         if (overflowHandler != null)
            blockingQueueSender.setOverflowHandler(overflowHandler);
         senders.put(destination, blockingQueueSender);
      }
      
      return blockingQueueSender;
   }
   
   @Override
   public synchronized void reclaim(Destination destination)
   {
      BlockingQueueSender sender = senders.get(destination);
      if (sender != null)
      {
         sender.shuttingDown();
         senders.remove(destination);
      }
   }
   
   @Override
   public synchronized void stop()
   {
      for (BlockingQueueSender sender : senders.values())
         sender.shuttingDown();
   }

}
