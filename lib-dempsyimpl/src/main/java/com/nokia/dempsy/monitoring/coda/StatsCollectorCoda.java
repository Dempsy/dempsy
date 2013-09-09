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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

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
   public static final String GAGE_LAST_OUTPUT_MILLIS = "outputInvoke-lastCycleMillis";
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
      GAGE_LAST_OUTPUT_MILLIS,
      TM_MP_PREIN,         TM_MP_HANDLE,
      TM_MP_OUTPUT,        TM_MP_EVIC
      
   };

   private final Meter messagesReceived;
   private final Histogram bytesReceived;
   private final Meter messagesDiscarded;
   private final Meter messagesCollisions;
   private final Meter messagesDispatched;
   private final Meter messagesFwFailed;
   private final Meter messagesMpFailed;
   private final Meter messagesProcessed;
   private final Meter messagesSent;
   private final Histogram bytesSent;
   private final Meter messagesUnsent;
   private final AtomicInteger inProcessMessages;
   private final AtomicLong numberOfMPs;
   private final AtomicLong lastOutputCycleMillis;
   private final Meter mpsCreated;
   private final Meter mpsDeleted;
   
   private Timer preInstantiationDuration;
   private Timer mpHandleMessageDuration;
   private Timer outputInvokeDuration;
   private Timer evictionInvokeDuration;
   
   private StatsCollector.Gauge currentMessagesPendingGauge;
   private StatsCollector.Gauge currentMessagesOutPendingGauge;
   private ClusterId clusterId;
   private StatsCollectorFactoryCoda.MetricNamingStrategy namer;
   
   {
      StatsCollector.Gauge zeroGauge = new Gauge()
      {
         @Override
         public long value()
         {
            return 0;
         }
      };
      
      currentMessagesPendingGauge = zeroGauge;
      currentMessagesOutPendingGauge = zeroGauge;
   }

   public StatsCollectorCoda(ClusterId clusterId, StatsCollectorFactoryCoda.MetricNamingStrategy namer)
   {
      this.namer = namer;
      this.clusterId = clusterId;
      messagesReceived = Metrics.newMeter(createName(MN_MSG_RCVD), "messages", TimeUnit.SECONDS);
      bytesReceived = Metrics.newHistogram(createName(MN_BYTES_RCVD));
      messagesDiscarded = Metrics.newMeter(createName(MN_MSG_DISCARD), "messages", TimeUnit.SECONDS);
      messagesCollisions = Metrics.newMeter(createName(MN_MSG_COLLISION), "messages", TimeUnit.SECONDS);
      messagesDispatched = Metrics.newMeter(createName(MN_MSG_DISPATCH), "messages", TimeUnit.SECONDS);
      messagesFwFailed = Metrics.newMeter(createName(MN_MSG_FWFAIL), "messages", TimeUnit.SECONDS);
      messagesMpFailed = Metrics.newMeter(createName(MN_MSG_MPFAIL), "messages", TimeUnit.SECONDS);
      messagesProcessed = Metrics.newMeter(createName(MN_MSG_PROC), "messages", TimeUnit.SECONDS);
      messagesSent = Metrics.newMeter(createName(MN_MSG_SENT), "messages", TimeUnit.SECONDS);
      bytesSent = Metrics.newHistogram(createName(MN_BYTES_SENT));
      messagesUnsent = Metrics.newMeter(createName(MN_MSG_UNSENT), "messsages", TimeUnit.SECONDS);
      inProcessMessages = new AtomicInteger();
      Metrics.newGauge(createName(GAGE_MPS_IN_PROCESS), new com.yammer.metrics.core.Gauge<Integer>() {
         @Override
         public Integer value()
         {
            return inProcessMessages.get();
         }
      });

      numberOfMPs = new AtomicLong();
      lastOutputCycleMillis = new AtomicLong();
      mpsCreated = Metrics.newMeter(createName(MN_MP_CREATE), "instances", TimeUnit.SECONDS);
      mpsDeleted = Metrics.newMeter(createName(MN_MP_DELETE), "instances", TimeUnit.SECONDS);
      Metrics.newGauge(createName(GAGE_MPS), new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value() {
            return numberOfMPs.get();
         }
      });
      
      Metrics.newGauge(createName(GAGE_MSG_PENDING), new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value()
         {
            return StatsCollectorCoda.this.currentMessagesPendingGauge.value();
         }
      });

      Metrics.newGauge(createName(GAGE_MSG_OUT_PENDING), new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value()
         {
            return StatsCollectorCoda.this.currentMessagesOutPendingGauge.value();
         }
      });
      
      Metrics.newGauge(createName(GAGE_LAST_OUTPUT_MILLIS), new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value()
         {
            return StatsCollectorCoda.this.lastOutputCycleMillis.get();
         }
      });

      preInstantiationDuration = Metrics.newTimer(createName(TM_MP_PREIN), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      mpHandleMessageDuration = Metrics.newTimer(createName(TM_MP_HANDLE), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      outputInvokeDuration = Metrics.newTimer(createName(TM_MP_OUTPUT), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      evictionInvokeDuration = Metrics.newTimer(createName(TM_MP_EVIC), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
   }
   
   public ClusterId getClusterId() { return clusterId; }
   
   public MetricName createName(String metric) { return namer.createName(clusterId, metric); }

   protected MetricsRegistry getMetricsRegistry()
   {
      return Metrics.defaultRegistry();
   }

   @Override
   public void messageReceived(byte[] message) {
      messagesReceived.mark();
      bytesReceived.update(message.length);
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
   public void messageSent(byte[] message) {
      messagesSent.mark();
      bytesSent.update(message.length);
   }

   @Override
   public void messageNotSent()
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
      return messagesProcessed.count();
   }

   @Override
   public long getDispatchedMessageCount()
   {
      return messagesDispatched.count();
   }

   @Override
   public long getMessageFailedCount()
   {
      return messagesFwFailed.count() + messagesMpFailed.count();
   }

   @Override
   public long getDiscardedMessageCount()
   {
      return messagesDiscarded.count();
   }

   @Override
   public long getMessageCollisionCount()
   {
      return messagesCollisions.count();
   }

   @Override
   public int getInFlightMessageCount()
   {
      return inProcessMessages.get();
   }

   @Override
   public void stop()
   {
      Metrics.shutdown();
      for (String name: METRIC_NAMES){
          Metrics.defaultRegistry().removeMetric(createName(name));
      }
   }

   private static class CodaTimerContext implements StatsCollector.TimerContext
   {
      private com.yammer.metrics.core.TimerContext ctx;

      private CodaTimerContext(com.yammer.metrics.core.TimerContext ctx) { this.ctx = ctx; }

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
      return preInstantiationDuration.meanRate();
   }

   @Override
   public StatsCollector.TimerContext handleMessageStarted()
   {
      return new CodaTimerContext(mpHandleMessageDuration.time());
   }


   @Override
   public StatsCollector.TimerContext outputInvokeStarted() {
      return new CodaTimerContext(outputInvokeDuration.time())
      {
         final long startTimeMillis = System.currentTimeMillis();
         
         @Override
         public void stop() { super.stop(); lastOutputCycleMillis.set(System.currentTimeMillis() - startTimeMillis); }
      };
   }

   @Override
   public double getOutputInvokeDuration()
   {
      return outputInvokeDuration.meanRate();
   }

   @Override
   public StatsCollector.TimerContext evictionPassStarted() {
      return new CodaTimerContext(evictionInvokeDuration.time());
   }

   @Override
   public double getEvictionDuration() {
      return evictionInvokeDuration.meanRate();
   }

   @Override
   public long getMessagesNotSentCount()
   {
      return messagesUnsent.count();
   }

   @Override
   public long getMessagesSentCount()
   {
      return messagesSent.count();
   }
   
   @Override
   public long getMessagesReceivedCount()
   {
      return messagesReceived.count();
   }
   
   @Override
   public long getMessageProcessorsCreated()
   {
      return mpsCreated.count();
   }
   
   @Override
   public long getMessageProcessorCount()
   {
      return numberOfMPs.get();
   }
   
   @Override
   public long getMessageBytesSent()
   {
      return Math.round(bytesSent.count() * bytesSent.mean());
   }

   @Override
   public long getMessageBytesReceived()
   {
      return Math.round(bytesReceived.count() * bytesReceived.mean());
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
