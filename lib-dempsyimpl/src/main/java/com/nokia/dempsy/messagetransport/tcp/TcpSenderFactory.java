package com.nokia.dempsy.messagetransport.tcp;

import java.util.Map;

import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.util.ForwardingSenderFactory;
import com.nokia.dempsy.messagetransport.util.ReceiverIndexedDestination;
import com.nokia.dempsy.messagetransport.util.SenderConnection;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TcpSenderFactory extends ForwardingSenderFactory
{
   protected final long socketWriteTimeoutMillis;
   protected final boolean batchOutgoingMessages;
   protected final long maxNumberOfQueuedOutbound;
   
   public TcpSenderFactory(Map<Destination,SenderConnection> connections, StatsCollector statsCollector,
         long maxNumberOfQueuedOutbound, long socketWriteTimeoutMillis, boolean batchOutgoingMessages)
   {
      super(connections,statsCollector,null);
      this.socketWriteTimeoutMillis = socketWriteTimeoutMillis;
      this.batchOutgoingMessages = batchOutgoingMessages;
      this.maxNumberOfQueuedOutbound = maxNumberOfQueuedOutbound;
   }

   @Override
   protected ReceiverIndexedDestination makeBaseDestination(ReceiverIndexedDestination destination)
   {
      return ((TcpDestination)destination).baseDestination();
   }

   @Override
   protected SenderConnection makeNewSenderConnection(ReceiverIndexedDestination baseDestination)
   {
      return new TcpSenderConnection((TcpDestination)baseDestination,maxNumberOfQueuedOutbound, socketWriteTimeoutMillis, batchOutgoingMessages);
   }

}