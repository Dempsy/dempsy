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

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
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
   private String scope;
   
   private Timer preInstantiationDuration;
   private Timer mpHandleMessageDuration;
   private Timer outputInvokeDuration;
   private Timer evictionInvokeDuration;
   
   private StatsCollector.Gauge currentMessagesPendingGauge;
   private StatsCollector.Gauge currentMessagesOutPendingGauge;
   
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


   public StatsCollectorCoda(ClusterId clusterId)
   {
      scope = clusterId.getApplicationName() + "." + clusterId.getMpClusterName();
      messagesReceived = Metrics.newMeter(Dempsy.class, MN_MSG_RCVD, scope, "messages", TimeUnit.SECONDS);
      bytesReceived = Metrics.newMeter(Dempsy.class, MN_BYTES_RCVD, scope, "bytes", TimeUnit.SECONDS);
      messagesDiscarded = Metrics.newMeter(Dempsy.class, MN_MSG_DISCARD, scope, "messages", TimeUnit.SECONDS);
      messagesCollisions = Metrics.newMeter(Dempsy.class, MN_MSG_COLLISION, scope, "messages", TimeUnit.SECONDS);
      messagesDispatched = Metrics.newMeter(Dempsy.class, MN_MSG_DISPATCH, scope, "messages", TimeUnit.SECONDS);
      messagesFwFailed = Metrics.newMeter(Dempsy.class, MN_MSG_FWFAIL, scope, "messages", TimeUnit.SECONDS);
      messagesMpFailed = Metrics.newMeter(Dempsy.class, MN_MSG_MPFAIL, scope, "messages", TimeUnit.SECONDS);
      messagesProcessed = Metrics.newMeter(Dempsy.class, MN_MSG_PROC, scope, "messages", TimeUnit.SECONDS);
      messagesSent = Metrics.newMeter(Dempsy.class, MN_MSG_SENT, scope, "messages", TimeUnit.SECONDS);
      bytesSent = Metrics.newMeter(Dempsy.class, MN_BYTES_SENT, scope, "bytes", TimeUnit.SECONDS);
      messagesUnsent = Metrics.newMeter(Dempsy.class, MN_MSG_UNSENT, scope, "messsages", TimeUnit.SECONDS);
      inProcessMessages = new AtomicInteger();
      Metrics.newGauge(Dempsy.class, GAGE_MPS_IN_PROCESS, scope, new com.yammer.metrics.core.Gauge<Integer>() {
         @Override
         public Integer value()
         {
            return inProcessMessages.get();
         }
      });

      numberOfMPs = new AtomicLong();
      mpsCreated = Metrics.newMeter(Dempsy.class, MN_MP_CREATE, scope, "instances", TimeUnit.SECONDS);
      mpsDeleted = Metrics.newMeter(Dempsy.class, MN_MP_DELETE, scope, "instances", TimeUnit.SECONDS);
      Metrics.newGauge(Dempsy.class, GAGE_MPS, scope, new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value() {
            return numberOfMPs.get();
         }
      });
      
      Metrics.newGauge(Dempsy.class, GAGE_MSG_PENDING, scope, new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value()
         {
            return StatsCollectorCoda.this.currentMessagesPendingGauge.value();
         }
      });

      Metrics.newGauge(Dempsy.class, GAGE_MSG_OUT_PENDING, scope, new com.yammer.metrics.core.Gauge<Long>() {
         @Override
         public Long value()
         {
            return StatsCollectorCoda.this.currentMessagesOutPendingGauge.value();
         }
      });

      preInstantiationDuration = Metrics.newTimer(Dempsy.class, TM_MP_PREIN,
            scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      mpHandleMessageDuration = Metrics.newTimer(Dempsy.class, TM_MP_HANDLE,
            scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      outputInvokeDuration = Metrics.newTimer(Dempsy.class, TM_MP_OUTPUT,
            scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      evictionInvokeDuration = Metrics.newTimer(Dempsy.class, TM_MP_EVIC,
            scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

   }

   protected MetricsRegistry getMetricsRegistry()
   {
      return Metrics.defaultRegistry();
   }

   @Override
   public void messageReceived(byte[] message) {
      messagesReceived.mark();
      bytesReceived.mark(message.length);
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
      bytesSent.mark(message.length);
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
         Metrics.defaultRegistry().removeMetric(Dempsy.class, name, scope);
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
      return new CodaTimerContext(outputInvokeDuration.time());
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
   public long getMessageBytesSent()
   {
      return bytesSent.count();
   }

   @Override
   public long getMessageBytesReceived()
   {
      return bytesReceived.count();
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
