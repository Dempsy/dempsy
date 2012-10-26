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

import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;

public class BlockingQueueSenderFactory implements SenderFactory
{
   private Map<Destination,BlockingQueueSender> senders = new HashMap<Destination,BlockingQueueSender>();
   private OverflowHandler overflowHandler = null;
   
   public void setOverflowHandler(OverflowHandler overflowHandler) { this.overflowHandler = overflowHandler; }
   
   @Override
   public synchronized Sender getSender(Destination destination) throws MessageTransportException
   {
      BlockingQueueSender blockingQueueSender = senders.get(destination);
      if (blockingQueueSender == null)
      {
         blockingQueueSender = new BlockingQueueSender();
         blockingQueueSender.setQueue(((BlockingQueueDestination)destination).queue);
         if (overflowHandler != null)
            blockingQueueSender.setOverflowHandler(overflowHandler);
         senders.put(destination, blockingQueueSender);
      }
      
      return blockingQueueSender;
   }
   
   public void stop()
   {
      for (BlockingQueueSender sender : senders.values())
         sender.shuttingDown();
   }

}
