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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TcpTransport implements Transport
{
   private OverflowHandler overflowHandler = null;
   private boolean failFast = false;
   
   private boolean batchOutgoingMessages = false;
   private long socketWriteTimeoutMillis = 30000; 
   private long maxNumberOfQueuedOutbound = 10000;
   private TcpServer server = new TcpServer();
   private ConcurrentHashMap<Destination,TcpSenderConnection> connections = new ConcurrentHashMap<Destination, TcpSenderConnection>();

   @Override
   public SenderFactory createOutbound(DempsyExecutor executor, StatsCollector statsCollector) throws MessageTransportException
   {
      return new TcpSenderFactory(connections,statsCollector, maxNumberOfQueuedOutbound, socketWriteTimeoutMillis, batchOutgoingMessages);
   }

   @Override
   public Receiver createInbound(DempsyExecutor executor) throws MessageTransportException
   {
      TcpReceiver receiver = new TcpReceiver(server,executor,failFast);
      receiver.setOverflowHandler(overflowHandler);
      return receiver;
   }

   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler)
   {
      this.overflowHandler = overflowHandler;
   }
   
   public void setFailFast(boolean failFast) { this.failFast = failFast; }
   
   /**
    * <p>By default the {@link TcpSender} sends and flushes one message at a time. You can have
    * any {@link TcpSender} that results from the {@link TcpSenderFactory} from this instance
    * of the {@link TcpTransport} batch up all pending messages prior to flushing the output 
    * buffer.</p>
    * 
    * <p>The drawback here is that messages can be lost that have been marked as Sent but it can
    * perform better.</p>
    */
   public void setBatchOutgoingMessages(boolean batchOutgoingMessages)
   {
      this.batchOutgoingMessages = batchOutgoingMessages;
   }

   /**
    * Because the {@link TcpSender} does a blocking write, this will set a timeout on the 
    * blocking write. The timeout measurement begins with a 'flush' of the data and ends when
    * either the timeout expires or when the flush is completed.
    */
   public void setSocketWriteTimeoutMillis(long socketWriteTimeoutMillis)
   {
      this.socketWriteTimeoutMillis = socketWriteTimeoutMillis;
   }
   
   /**
    * <p>The {@link TcpSender} sends data from a dedicated thread and reads from a queue. This will set
    * the maximum number of pending sends. When the maximum number of pending sends is reached, the oldest
    * data will be discarded.</p>
    * 
    * <p>Setting the value to -1 will allow the queue to be unbounded. The default is 10000.</p>
    */
   public void setMaxNumberOfQueuedOutbound(long maxNumberOfQueuedOutbound)
   {
      this.maxNumberOfQueuedOutbound = maxNumberOfQueuedOutbound;
   }

   public static InetAddress getFirstNonLocalhostInetAddress() throws SocketException
   {
      Enumeration<NetworkInterface> netInterfaces=NetworkInterface.getNetworkInterfaces();
      while(netInterfaces.hasMoreElements()){
         NetworkInterface networkInterface = (NetworkInterface)netInterfaces.nextElement();
         for (Enumeration<InetAddress> loopInetAddress = networkInterface.getInetAddresses(); loopInetAddress.hasMoreElements(); )
         {
            InetAddress tempInetAddress = loopInetAddress.nextElement();
            if (!tempInetAddress.isLoopbackAddress() && tempInetAddress instanceof Inet4Address)
               return tempInetAddress;
         }
      }
      return null;
   }
   
   public static InetAddress getInetAddressBestEffort()
   {
      InetAddress ret = null;
      
      try { ret = getFirstNonLocalhostInetAddress(); }
      catch (SocketException e) { ret = null; }
      
      if (ret == null)
      {
         try { ret = InetAddress.getLocalHost(); }
         catch (UnknownHostException e) { ret = null; }
      }
      return ret;
   }

}
