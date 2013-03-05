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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.monitoring.StatsCollector;

public class ForwardedReceiver implements Receiver
{
   private static Logger logger = LoggerFactory.getLogger(ForwardedReceiver.class);
   
   protected final DempsyExecutor executor;
   protected final Server server;
   protected final AtomicBoolean isStarted = new AtomicBoolean(false);
   protected final boolean iShouldStopExecutor;
   
   protected ReceiverIndexedDestination destination;
   private String destinationString = "";

   private Listener messageTransportListener;
   private OverflowHandler overflowHandler = null;
   private boolean failFast;
   
   protected StatsCollector statsCollector;

   public ForwardedReceiver(final Server server, final DempsyExecutor executor, 
         final boolean failFast, final boolean stopExecutor) throws MessageTransportException 
   {
      this.server = server;
      this.executor = executor;
      this.failFast = failFast;
      if (executor == null)
         throw new MessageTransportException("You must set the executor on a " + 
               ForwardedReceiver.class.getSimpleName());
      this.iShouldStopExecutor = stopExecutor;
   }
   
   public DempsyExecutor getExecutor() { return executor; }
   
   public Server getServer() { return server; }
   
   @Override
   public synchronized void start() throws MessageTransportException
   {
      if (isStarted())
         return;
      
      getDestination();
      
      // check to see that the overflowHandler and the failFast setting are consistent.
      if (!failFast && overflowHandler != null)
         logger.warn("TcpReceiver/TcpTransport is configured with an OverflowHandler that will never be used because it's also configured to NOT 'fail fast' so it will always block waiting for messages to be processed.");
      
      setPendingGague();
      
      isStarted.set(true);
   }
   
   @Override
   public boolean getFailFast() { return failFast; }
   
   protected void handleMessage(byte[] messageBytes)
   {
      if ( messageTransportListener != null)
      {
         try
         {
            final byte[] pass = messageBytes;
            executor.submitLimited(new DempsyExecutor.Rejectable<Object>()
            {
               byte[] message = pass;

               @Override
               public Object call() throws Exception
               {
                  boolean messageSuccess = messageTransportListener.onMessage( message, failFast );
                  if (overflowHandler != null && !messageSuccess)
                     overflowHandler.overflow(message);
                  return null;
               }

               @Override
               public void rejected()
               {
                  if (statsCollector != null)
                     statsCollector.messageDiscarded(message);
               }
            });
            
//            if (logger.isTraceEnabled())
//               logger.error(destinationString + " has " + executor.getNumberPending() + " pending messages.");

         }
         catch (Throwable se)
         {
            String messageAsString;
            try { messageAsString = (messageBytes == null ? "null" : messageBytes.toString()); } catch (Throwable th) { messageAsString = "(failed to convert message to a string)"; }
            logger.error("Unexpected listener exception on adaptor for " + destinationString +
                  " trying to process a message of type " + messageBytes.getClass().getSimpleName() + " with a value of " +
                  messageAsString + " using listener " + messageTransportListener.getClass().getSimpleName(), se);
         }
      }
   }
   
   public void setOverflowHandler(OverflowHandler handler) { this.overflowHandler = handler; }
   
   public synchronized void shutdown()
   {
      if (destination != null) server.unregister(destination);
      
      try { if ( messageTransportListener != null) messageTransportListener.transportShuttingDown(); }
      catch (Throwable th)
      { logger.error("Listener threw exception when being notified of shutdown on " + destination, th); }
      
      if (executor != null && iShouldStopExecutor)
         executor.shutdown();

      isStarted.set(false);
   }
   
   public synchronized boolean isStarted() { return isStarted.get(); }
   
   @Override
   public synchronized ReceiverIndexedDestination getDestination() throws MessageTransportException
   {
      if (destination == null)
         destination = server.register(this);
      
      destinationString = destination.toString();

      return destination;
   }
   
   @Override
   public void setListener(Listener messageTransportListener )
   {
      this.messageTransportListener = messageTransportListener;
   }
   
   @Override
   public synchronized void setStatsCollector(StatsCollector statsCollector) 
   {
      this.statsCollector = statsCollector;
      setPendingGague();
   }
   
   protected void setPendingGague()
   {
      if (statsCollector != null && executor != null)
      {
         statsCollector.setMessagesPendingGauge(new StatsCollector.Gauge()
         {
            @Override
            public long value()
            {
               return executor.getNumberPending();
            }
         });
      }
   }
}
