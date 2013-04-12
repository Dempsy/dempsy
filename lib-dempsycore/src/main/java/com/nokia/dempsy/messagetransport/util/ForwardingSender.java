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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.monitoring.StatsCollector;

/**
 * The is a concrete Sender implementation that forwards messages
 * to a SenderConnection.
 */
public class ForwardingSender implements Sender
{
   private static Logger logger = LoggerFactory.getLogger(ForwardingSender.class);
   private static final AtomicLong sequence = new AtomicLong(0);
   
   protected final ReceiverIndexedDestination destination;
   protected final StatsCollector statsCollector;
   protected final SenderConnection connection;
   protected final String thisNodeDescription;
   
   public ForwardingSender(SenderConnection connection, ReceiverIndexedDestination destination,
         StatsCollector statsCollector, String desc) throws MessageTransportException
   {
      this.thisNodeDescription = desc;
      this.connection = connection;
      this.destination = destination;
      this.statsCollector = statsCollector;
      init();
   }
   
   protected void init()
   {
      if (this.statsCollector != null)
      {
         this.statsCollector.setMessagesOutPendingGauge(new StatsCollector.Gauge()
         {
            @Override
            public long value()
            {
               return ForwardingSender.this.connection.getQ().size();
            }
         });
      }
      connection.start(this);
      
      if (logger.isTraceEnabled())
         logger.trace("Created " + this);
   }
   
   public SenderConnection getConnection() { return connection; }
   
   public final class Enqueued
   {
      public final MessageBufferOutput message;
      public final int numBytes;
      
      public Enqueued(MessageBufferOutput message) { this.message = message; this.numBytes = message.getPosition(); }
      
      public final void messageNotSent() { if (statsCollector != null) statsCollector.messageNotSent(message.getBuffer()); }
      public final void messageSent() { if (statsCollector != null) statsCollector.messageSent(numBytes); }
      public final int getReceiverIndex() { return destination.getReceiverIndex(); }
   }
   
   @Override
   public void send(MessageBufferOutput buffer) throws MessageTransportException
   {
      try { connection.getQ().put(new Enqueued(buffer)); }
      catch (InterruptedException e)
      {
         if (statsCollector != null) statsCollector.messageNotSent(buffer.getBuffer());
         throw new MessageTransportException("Failed to enqueue message to " + destination + ".",e);
      }
   }
   
   @Override
   public String toString() { return "Sending from " + 
         (thisNodeDescription == null ? ("sender #" + sequence.getAndIncrement()) : thisNodeDescription)
         + " to " + destination; }
   
   public void stop() { connection.stop(this); }
   
}
