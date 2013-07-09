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

package com.nokia.dempsy.messagetransport.tcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TcpSenderFactory implements SenderFactory
{
   private volatile boolean isStopped = false;
   protected StatsCollector statsCollector = null;
   private Map<Destination,TcpSender> senders = new HashMap<Destination, TcpSender>();
   
   protected final long socketWriteTimeoutMillis;
   protected final long batchOutgoingMessagesDelayMillis;
   protected final long maxNumberOfQueuedOutbound;
   protected final int mtu;
   
   protected TcpSenderFactory(StatsCollector statsCollector, long maxNumberOfQueuedOutbound, long socketWriteTimeoutMillis, long batchOutgoingMessagesDelayMillis)
   {
      this.statsCollector = statsCollector;
      this.socketWriteTimeoutMillis = socketWriteTimeoutMillis;
      this.batchOutgoingMessagesDelayMillis = batchOutgoingMessagesDelayMillis;
      this.maxNumberOfQueuedOutbound = maxNumberOfQueuedOutbound;
      this.mtu = TcpTransport.determineMtu();
   }

   @Override
   public Sender getSender(Destination destination) throws MessageTransportException
   {
      if (isStopped == true)
         throw new MessageTransportException("getSender called for the destination " + SafeString.valueOf(destination) + 
               " on a stopped " + SafeString.valueOfClass(this));
      
      TcpSender sender;
      synchronized(senders)
      {
         sender = senders.get(destination);
         if (sender == null)
         {
            sender = makeTcpSender( (TcpDestination)destination );
            senders.put( destination, sender );
         }
      }
      return sender;
   }
   
   @Override
   public void stop() 
   {
      isStopped = true;
      List<TcpSender> scol = new ArrayList<TcpSender>();
      synchronized(senders)
      {
         scol.addAll(senders.values());
         senders.clear();
      }
      for (TcpSender sender : scol)
         sender.stop();
   }
   
   /**
    * This method is here for testing. It allows me to create a fake output stream that 
    * I can disrupt to test the behavior of network failures.
    */
   protected TcpSender makeTcpSender(TcpDestination destination) throws MessageTransportException
   {
      return new TcpSender( (TcpDestination)destination, statsCollector, maxNumberOfQueuedOutbound, socketWriteTimeoutMillis, batchOutgoingMessagesDelayMillis, mtu );
   }

}
