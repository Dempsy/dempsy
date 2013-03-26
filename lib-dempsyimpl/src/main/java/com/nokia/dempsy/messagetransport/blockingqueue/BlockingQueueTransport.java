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
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;

public class BlockingQueueTransport implements Transport
{
   interface BlockingQueueFactory
   {
      public BlockingQueue<MessageBufferInput> createBlockingQueue();
   }
   
   private BlockingQueueFactory queueSource = null;

   @Override
   public SenderFactory createOutbound(DempsyExecutor executor, StatsCollector statsCollector, String desc)
   {
      return new BlockingQueueSenderFactory(statsCollector);
   }

   @Override
   public Receiver createInbound(DempsyExecutor executor, String desc) throws MessageTransportException
   {
      BlockingQueueAdaptor ret = new BlockingQueueAdaptor(executor);
      ret.setQueue(getNewQueue());
      return ret;
   }
   
   private BlockingQueue<MessageBufferInput> getNewQueue()
   {
      return (queueSource == null) ? new LinkedBlockingQueue<MessageBufferInput>() : queueSource.createBlockingQueue();
   }
}
