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

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.util.SenderConnection;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TcpSender implements Sender
{
   protected TcpDestination destination;

   private StatsCollector statsCollector = null;
   protected final TcpSenderConnection connection;

   protected TcpSender(TcpSenderConnection connection,TcpDestination destination, StatsCollector statsCollector) throws MessageTransportException
   {
      this.connection = connection;
      this.destination = destination;
      this.statsCollector = statsCollector;
      if (this.statsCollector != null)
      {
         this.statsCollector.setMessagesOutPendingGauge(new StatsCollector.Gauge()
         {
            @Override
            public long value()
            {
               return TcpSender.this.connection.getQ().size();
            }
         });
      }
      connection.start(this);
   }
   
   public class Enqueued extends SenderConnection.Enqueued
   {
      public Enqueued(byte[] messageBytes) { this.messageBytes = messageBytes; }
      
      public void messageNotSent() { if (statsCollector != null) statsCollector.messageNotSent(messageBytes); }
      public void messageSent() { if (statsCollector != null) statsCollector.messageSent(messageBytes); }
      public int getReceiverIndex() { return destination.getReceiverIndex(); }
   }
   
   @Override
   public void send(byte[] messageBytes) throws MessageTransportException
   {
      try { connection.getQ().put(new Enqueued(messageBytes)); }
      catch (InterruptedException e)
      {
         if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
         throw new MessageTransportException("Failed to enqueue message to " + destination + ".",e);
      }
   }
   
   @Override
   public String toString() { return "TcpSender to " + destination; }
   
   protected void stop()
   {
      connection.stop(this);
   }
   
}
