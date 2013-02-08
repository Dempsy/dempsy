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
import java.util.concurrent.LinkedBlockingQueue;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;

public class BlockingQueueTransport implements Transport
{
   interface BlockingQueueFactory
   {
      public BlockingQueue<byte[]> createBlockingQueue();
   }
   
   private BlockingQueueFactory queueSource = null;
   private OverflowHandler overflowHandler = null;

   @Override
   public SenderFactory createOutbound(DempsyExecutor executor, StatsCollector statsCollector)
   {
      BlockingQueueSenderFactory ret = new BlockingQueueSenderFactory(statsCollector);
      if (overflowHandler != null)
         ret.setOverflowHandler(overflowHandler);
      return ret;
   }

   @Override
   public Receiver createInbound(DempsyExecutor executor) throws MessageTransportException
   {
      BlockingQueueAdaptor ret = new BlockingQueueAdaptor();
      ret.setQueue(getNewQueue());
      if (overflowHandler != null)
         ret.setOverflowHandler(overflowHandler);
      return ret;
   }
   
   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler)
   {
      this.overflowHandler = overflowHandler;
   }
   
   private BlockingQueue<byte[]> getNewQueue()
   {
      return (queueSource == null) ? new LinkedBlockingQueue<byte[]>() : queueSource.createBlockingQueue();
   }
}
