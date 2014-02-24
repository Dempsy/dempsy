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

package com.nokia.dempsy.messagetransport.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.monitoring.StatsCollector;

public abstract class ForwardingSenderFactory implements SenderFactory
{
   private static final Logger logger = LoggerFactory.getLogger(ForwardingSenderFactory.class);

   // referenced only while the monitor is held on senders
   protected StatsCollector statsCollector = null;
   private boolean isStopped = false;
   private final Map<Destination,SenderConnection> connections;
   private final Map<Destination,ForwardingSender> senders = new HashMap<Destination, ForwardingSender>();
   private final String thisNodeDescription;
   protected final ConcurrentLinkedQueue<MessageBufferOutput> pool = new ConcurrentLinkedQueue<MessageBufferOutput>();
   
   protected ForwardingSenderFactory(Map<Destination,SenderConnection> connections, StatsCollector statsCollector, String thisNodeDescription)
   {
      this.statsCollector = statsCollector;
      this.connections = connections;
      this.thisNodeDescription = thisNodeDescription;
   }

   @Override
   public Sender getSender(Destination destination) throws MessageTransportException
   {
      ForwardingSender sender;
      synchronized(senders)
      {
         if (isStopped == true)
            throw new MessageTransportException("getSender called for the destination " + SafeString.valueOf(destination) + 
                  " on a stopped " + SafeString.valueOfClass(this));
         
         sender = senders.get(destination);
         if (sender == null)
         {
            sender = makeTcpSender( (ReceiverIndexedDestination)destination );
            senders.put( destination, sender );
         }
      }
      return sender;
   }
   
   @Override
   public void stopDestination(Destination destination)
   {
      ForwardingSender sender;
      synchronized (senders) { sender = senders.remove(destination); }
      if (sender != null)
         sender.stop();
   }
   
   @Override
   public void shutdown() 
   {
      List<ForwardingSender> scol = new ArrayList<ForwardingSender>();
      synchronized(senders)
      {
         isStopped = true;
         scol.addAll(senders.values());
         senders.clear();
      }
      for (ForwardingSender sender : scol)
         sender.stop();
   }
   
   protected abstract ReceiverIndexedDestination makeBaseDestination(ReceiverIndexedDestination destination);
   
   protected abstract SenderConnection makeNewSenderConnection(ReceiverIndexedDestination baseDestination);
   
   /**
    * This method is here for testing. It allows me to create a fake output stream that 
    * I can disrupt to test the behavior of network failures.
    * 
    * Called only with the lock held.
    */
   protected ForwardingSender makeTcpSender(ReceiverIndexedDestination destination) throws MessageTransportException
   {
      ReceiverIndexedDestination baseDestination = makeBaseDestination(destination);
      SenderConnection connection = connections.get(baseDestination);
      if (connection == null)
      {
         connection = makeNewSenderConnection(baseDestination);
         connections.put(baseDestination, connection);
      }
      if (logger.isTraceEnabled())
         logger.trace(toString() + ", making Sender for " + destination);
      return new ForwardingSender(connection, destination, statsCollector,thisNodeDescription);
   }
   
   private static class ConstructableMessageBufferOutput extends MessageBufferOutput { public ConstructableMessageBufferOutput() { super(32); } }
   
   @Override
   public MessageBufferOutput prepareMessage()
   {
//      final MessageBufferOutput ret = pool.poll();
//      if (ret == null)
//      {
         final MessageBufferOutput tret = new ConstructableMessageBufferOutput();
         tret.setPosition(1);
         return tret;
//      }
//      ret.setPosition(1); // reserve 1 byte for the receiver index.
//      return ret;
   }
   
   protected void returnMessageBufferOutput(MessageBufferOutput ret) { /*ret.reset(); pool.offer(ret);*/ }

}
