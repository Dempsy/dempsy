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

package com.nokia.dempsy.monitoring.coda;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.monitoring.StatsCollector;

/**
 * An implementation of the Dempsy StatsCollector
 * using Coda Hale's Metrics package.
 * @link {http://metrics.codahale.com}
 * 
 * By default, all stats are visible via JMX, but can be
 * output by other means, where Grapite was the driving
 * factor in moving from simple annontated MBeans.
 * 
 */
public class StatsCollectorCoda implements StatsCollector, MetricGetters
{
   private final MetricRegistry registry;
   
   // Metric Names
   public static final String MN_MSG_RCVD = "messages-received";
   public static final String MN_BYTES_RCVD = "bytes-received";
   public static final String MN_MSG_DISCARD = "messages-discarded";
   public static final String MN_MSG_DISPATCH = "messages-dispatched";
   public static final String MN_MSG_FWFAIL = "messages-dempsy-failed";
   public static final String MN_MSG_MPFAIL = "messages-mp-failed";
   public static final String MN_MSG_PROC = "messages-processed";
   public static final String MN_MSG_SENT = "messages-sent";
   public static final String MN_BYTES_SENT = "bytes-sent";
   public static final String MN_MSG_UNSENT = "messages-unsent";
   public static final String MN_MP_CREATE = "message-processors-created";
   public static final String MN_MP_DELETE = "message-processors-deleted";
   public static final String MN_MSG_COLLISION = "messages-collisions";
   public static final String GAGE_MPS_IN_PROCESS = "messages-in-process";
   public static final String GAGE_MPS = "message-processors";
   public static final String GAGE_MSG_PENDING = "messages-pending";
   public static final String GAGE_MSG_OUT_PENDING = "messages-out-pending";
   public static final String TM_MP_PREIN = "pre-instantiation-duration";
   public static final String TM_MP_HANDLE = "mp-handle-message-duration";
   public static final String TM_MP_OUTPUT = "outputInvoke-duration";
   public static final String TM_MP_EVIC = "evictionInvoke-duration";
   public static final String[] METRIC_NAMES = {
      MN_MSG_RCVD,         MN_BYTES_RCVD,
      MN_MSG_DISCARD,      MN_MSG_DISPATCH,
      MN_MSG_FWFAIL,       MN_MSG_MPFAIL,
      MN_MSG_PROC,         MN_MSG_SENT,
      MN_BYTES_SENT,       MN_MSG_UNSENT,
      MN_MP_CREATE,        MN_MP_DELETE,
      MN_MSG_COLLISION,
      GAGE_MPS_IN_PROCESS, GAGE_MPS,
      GAGE_MSG_PENDING,    GAGE_MSG_OUT_PENDING,
      TM_MP_PREIN,         TM_MP_HANDLE,
      TM_MP_OUTPUT,        TM_MP_EVIC
      
   };

   private Meter messagesReceived;
   private Meter bytesReceived;
   private Meter messagesDiscarded;
   private Meter messagesCollisions;
   private Meter messagesDispatched;
   private Meter messagesFwFailed;
   private Meter messagesMpFailed;
   private Meter messagesProcessed;
   private Meter messagesSent;
   private Meter bytesSent;
   private Meter messagesUnsent;
   private AtomicInteger inProcessMessages;
   private AtomicLong numberOfMPs;
   private Meter mpsCreated;
   private Meter mpsDeleted;
   
   private Timer preInstantiationDuration;
   private Timer mpHandleMessageDuration;
   private Timer outputInvokeDuration;
   private Timer evictionInvokeDuration;
   
   private StatsCollector.Gauge currentMessagesPendingGauge;
   private StatsCollector.Gauge currentMessagesOutPendingGauge;
   final private ClusterId clusterId;
   final private StatsCollectorFactoryCoda.MetricNamingStrategy namer;
   final private StatsCollectorFactoryCoda factory;
   
   {
      StatsCollector.Gauge tmp = new Gauge()
      {
         @Override
         public long value()
         {
            return 0;
         }
      };
      
      currentMessagesPendingGauge = tmp;
      currentMessagesOutPendingGauge = tmp;
   }

   public StatsCollectorCoda(ClusterId clusterId, MetricRegistry registry, StatsCollectorFactoryCoda.MetricNamingStrategy namer, StatsCollectorFactoryCoda factory)
   {
      this.factory = factory;
      this.registry = registry;
      this.namer = namer;
      this.clusterId = clusterId;
      messagesReceived = registry.meter(createName(MN_MSG_RCVD));
      bytesReceived = registry.meter(createName(MN_BYTES_RCVD));
      messagesDiscarded = registry.meter(createName(MN_MSG_DISCARD));
      messagesCollisions = registry.meter(createName(MN_MSG_COLLISION));
      messagesDispatched = registry.meter(createName(MN_MSG_DISPATCH));
      messagesFwFailed = registry.meter(createName(MN_MSG_FWFAIL));
      messagesMpFailed = registry.meter(createName(MN_MSG_MPFAIL));
      messagesProcessed = registry.meter(createName(MN_MSG_PROC));
      messagesSent = registry.meter(createName(MN_MSG_SENT));
      bytesSent = registry.meter(createName(MN_BYTES_SENT));
      messagesUnsent = registry.meter(createName(MN_MSG_UNSENT));
      inProcessMessages = new AtomicInteger();
      registry.register(createName(GAGE_MPS_IN_PROCESS), new com.codahale.metrics.Gauge<Integer>() {
         @Override
         public Integer getValue()
         {
            return inProcessMessages.get();
         }
      });

      numberOfMPs = new AtomicLong();
      mpsCreated = registry.meter(createName(MN_MP_CREATE));
      mpsDeleted = registry.meter(createName(MN_MP_DELETE));
      registry.register(createName(GAGE_MPS), new com.codahale.metrics.Gauge<Long>() {
         @Override
         public Long getValue() {
            return numberOfMPs.get();
         }
      });
      
      registry.register(createName(GAGE_MSG_PENDING), new com.codahale.metrics.Gauge<Long>() {
         @Override
         public Long getValue()
         {
            return StatsCollectorCoda.this.currentMessagesPendingGauge.value();
         }
      });

      registry.register(createName(GAGE_MSG_OUT_PENDING), new com.codahale.metrics.Gauge<Long>() {
         @Override
         public Long getValue()
         {
            return StatsCollectorCoda.this.currentMessagesOutPendingGauge.value();
         }
      });

      preInstantiationDuration = registry.timer(createName(TM_MP_PREIN));

      mpHandleMessageDuration = registry.timer(createName(TM_MP_HANDLE));

      outputInvokeDuration = registry.timer(createName(TM_MP_OUTPUT));

      evictionInvokeDuration = registry.timer(createName(TM_MP_EVIC));
   }
   
   public ClusterId getClusterId() { return clusterId; }
   
   protected String createName(String metric) { return namer.createName(clusterId, metric); }

   protected MetricRegistry getMetricsRegistry()
   {
      return registry;
   }

   @Override
   public void messageReceived(int length) {
      messagesReceived.mark();
      bytesReceived.mark(length);
   }

   @Override
   public void messageDiscarded(Object message) {
      messagesDiscarded.mark();
      inProcessMessages.decrementAndGet();
   }

   @Override
   public void messageCollision(Object message) {
      messagesCollisions.mark();
   }

   @Override
   public void messageDispatched(Object message) {
      messagesDispatched.mark();
      inProcessMessages.incrementAndGet();
   }

   @Override
   public void messageFailed(boolean mpFailure) {
      if (mpFailure)
         messagesMpFailed.mark();
      else
         messagesFwFailed.mark();
      inProcessMessages.decrementAndGet();
   }

   @Override
   public void messageProcessed(Object message) {
      messagesProcessed.mark();
      inProcessMessages.decrementAndGet();
   }

   @Override
   public void messageSent(int length) {
      messagesSent.mark();
      bytesSent.mark(length);
   }

   @Override
   public void messageNotSent(Object message)
   {
      messagesUnsent.mark();
   }
   @Override
   public void messageProcessorCreated(Object key) {
      mpsCreated.mark();
      numberOfMPs.incrementAndGet();
   }

   @Override
   public void messageProcessorDeleted(Object key) {
      mpsDeleted.mark();
      numberOfMPs.decrementAndGet();
   }


   /*
    *------------------------------------------------------------------------- 
    * Methods for enabling testing of the MP container only.
    * Serveral Unit tests read stats, which in the prior MBean implementation
    * was a free ride.  We do need to think about how this can be done better.
    *-------------------------------------------------------------------------
    */

   @Override
   public long getProcessedMessageCount()
   {
      return messagesProcessed.getCount();
   }

   @Override
   public long getDispatchedMessageCount()
   {
      return messagesDispatched.getCount();
   }

   @Override
   public long getMessageFailedCount()
   {
      return messagesFwFailed.getCount() + messagesMpFailed.getCount();
   }

   @Override
   public long getDiscardedMessageCount()
   {
      return messagesDiscarded.getCount();
   }

   @Override
   public long getMessageCollisionCount()
   {
      return messagesCollisions.getCount();
   }

   @Override
   public int getInFlightMessageCount()
   {
      return inProcessMessages.get();
   }

   @Override
   public void stop()
   {
      for (String name: METRIC_NAMES){
          registry.remove(createName(name));
      }
      factory.stop(this);
   }

   private static class CodaTimerContext implements StatsCollector.TimerContext
   {
      private com.codahale.metrics.Timer.Context ctx;

      private CodaTimerContext(com.codahale.metrics.Timer.Context ctx) { this.ctx = ctx; }

      @Override
      public void stop() { ctx.stop(); }
   }

   @Override
   public StatsCollector.TimerContext preInstantiationStarted()
   {
      return new CodaTimerContext(preInstantiationDuration.time());
   }

   @Override
   public double getPreInstantiationDuration()
   {
      return preInstantiationDuration.getMeanRate();
   }

   @Override
   public long getPreInstantiationCount()
   {
      return preInstantiationDuration.getCount();
   }

   @Override
   public StatsCollector.TimerContext handleMessageStarted()
   {
      return new CodaTimerContext(mpHandleMessageDuration.time());
   }


   @Override
   public StatsCollector.TimerContext outputInvokeStarted() {
      return new CodaTimerContext(outputInvokeDuration.time());
   }

   @Override
   public double getOutputInvokeDuration()
   {
      return outputInvokeDuration.getMeanRate();
   }

   @Override
   public StatsCollector.TimerContext evictionPassStarted() {
      return new CodaTimerContext(evictionInvokeDuration.time());
   }

   @Override
   public double getEvictionDuration() {
      return evictionInvokeDuration.getMeanRate();
   }

   @Override
   public long getMessagesNotSentCount()
   {
      return messagesUnsent.getCount();
   }

   @Override
   public long getMessagesSentCount()
   {
      return messagesSent.getCount();
   }
   
   @Override
   public long getMessagesReceivedCount()
   {
      return messagesReceived.getCount();
   }
   
   @Override
   public long getMessageProcessorsCreated()
   {
      return mpsCreated.getCount();
   }

   @Override
   public long getMessageProcessorCount()
   {
      return numberOfMPs.get();
   }

   @Override
   public long getMessageBytesSent()
   {
      return bytesSent.getCount();
   }

   @Override
   public long getMessageBytesReceived()
   {
      return bytesReceived.getCount();
   }

   @Override
   public synchronized void setMessagesPendingGauge(StatsCollector.Gauge currentMessagesPendingGauge)
   {
      if (currentMessagesPendingGauge != null)
         this.currentMessagesPendingGauge = currentMessagesPendingGauge;
   }

   @Override
   public synchronized void setMessagesOutPendingGauge(StatsCollector.Gauge currentMessagesOutPendingGauge)
   {
      if (currentMessagesOutPendingGauge != null)
         this.currentMessagesOutPendingGauge = currentMessagesOutPendingGauge;
   }

   @Override
   public long getMessagesPending()
   {
      return currentMessagesPendingGauge.value();
   }

   @Override
   public long getMessagesOutPending()
   {
      return currentMessagesOutPendingGauge.value();
   }
}
