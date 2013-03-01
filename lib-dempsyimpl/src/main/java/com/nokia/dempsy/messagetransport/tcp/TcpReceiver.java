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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.util.TransportUtils;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TcpReceiver implements Receiver
{
   private static Logger logger = LoggerFactory.getLogger(TcpReceiver.class);
   protected TcpDestination destination;
   private String destinationString = "";

   private Listener messageTransportListener;
   private OverflowHandler overflowHandler = null;
   private boolean failFast;
   private boolean blocking;
   
   protected DempsyExecutor executor = null;
   protected boolean iStartedIt = false;
   protected StatsCollector statsCollector;
   protected TcpServer server;
   protected AtomicBoolean isStarted = new AtomicBoolean(false);
   
   protected TcpReceiver(TcpServer server, DempsyExecutor executor, boolean failFast) 
   {
      this.server = server;
      this.executor = executor;
      this.failFast = failFast;
   }
   
   @Override
   public synchronized void start() throws MessageTransportException
   {
      if (isStarted())
         return;
      
      getDestination();
      
      // check to see that the overflowHandler and the failFast setting are consistent.
      if (!failFast && overflowHandler != null)
         logger.warn("TcpReceiver/TcpTransport is configured with an OverflowHandler that will never be used because it's also configured to NOT 'fail fast' so it will always block waiting for messages to be processed.");
      
      if (executor == null)
      {
         DefaultDempsyExecutor defexecutor = new DefaultDempsyExecutor();
         defexecutor.setCoresFactor(1.0);
         defexecutor.setAdditionalThreads(1);
         defexecutor.setMaxNumberOfQueuedLimitedTasks(10000);
         defexecutor.setBlocking(blocking);
         executor = defexecutor;
         iStartedIt = true;
         executor.start();
      }
      
      setPendingGague();
      
      isStarted.set(true);
   }
   
   @Override
   public boolean getFailFast() { return failFast; }
   
   public void setBlocking(boolean blocking)
   {
      this.blocking = blocking;
      if (executor != null && DefaultDempsyExecutor.class.isAssignableFrom(executor.getClass()))
         ((DefaultDempsyExecutor)executor).setBlocking(blocking);
   }
   
   public void handleMessage(byte[] messageBytes)
   {
      TransportUtils.handleMessage(messageBytes, executor, messageTransportListener, failFast, overflowHandler, statsCollector, logger, destinationString);
   }
   
   public void setOverflowHandler(OverflowHandler handler) { this.overflowHandler = handler; }
   
   public synchronized void shutdown()
   {
      server.unregister(destination);
      
      try { if ( messageTransportListener != null) messageTransportListener.transportShuttingDown(); }
      catch (Throwable th)
      { logger.error("Listener threw exception when being notified of shutdown on " + destination, th); }
      
      if (executor != null && iStartedIt)
         executor.shutdown();

      isStarted.set(false);
   }
   
   public synchronized boolean isStarted() { return isStarted.get(); }
   
   @Override
   public synchronized TcpDestination getDestination() throws MessageTransportException
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
