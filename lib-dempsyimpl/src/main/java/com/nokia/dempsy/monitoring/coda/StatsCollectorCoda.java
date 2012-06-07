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
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

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
public class StatsCollectorCoda implements StatsCollector {
	
   // Metric Names
   public static final String MN_MSG_RCVD = "messages-received";
   public static final String MN_MSG_DISCARD = "messages-discarded";
   public static final String MN_MSG_DISPATCH = "messages-dispatched";
   public static final String MN_MSG_FAIL = "messages-failed";
   public static final String MN_MSG_PROC = "messages-processed";
   public static final String MN_MSG_SENT = "messages-sent";
   public static final String MN_MSG_UNSENT = "messages-unsent";
   public static final String MN_MP_CREATE = "message-processors-created";
   public static final String MN_MP_DELETE = "message-processors-deleted";
   public static final String[] METRIC_NAMES = {
      MN_MSG_RCVD,
      MN_MSG_DISCARD,
      MN_MSG_DISPATCH,
      MN_MSG_FAIL,
      MN_MSG_PROC,
      MN_MSG_SENT,
      MN_MSG_UNSENT,
      MN_MP_CREATE,
      MN_MP_DELETE,
   };
   
	private Meter messagesReceived;
	private Meter messagesDiscarded;
	private Meter messagesDispatched;
	private Meter messagesFailed;
	private Meter messagesProcessed;
	private Meter messagesSent;
	private Meter messagesUnsent;
	private AtomicInteger inProcessMessages;
	@SuppressWarnings("unused")
	private Gauge<Integer> messagesInProcess;
	private AtomicLong numberOfMPs;
	private Meter mpsCreated;
	private Meter mpsDeleted;
	@SuppressWarnings("unused")
	private Gauge<Long> messageProcessors;
	private String scope;

	private Timer preInstantiationDuration;
	private TimerContext preInstantiationDurationContext;

	private Timer outputInvokeDuration;
	private TimerContext outputInvokeDurationContext;
	
	private Timer evictionInvokeDuration;
	private TimerContext evictionInvokeDurationContext;
  

	public StatsCollectorCoda(ClusterId clusterId)
	{
	   scope = clusterId.getApplicationName() + "." + clusterId.getMpClusterName();
	   messagesReceived = Metrics.newMeter(Dempsy.class, "messages-received",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesDiscarded = Metrics.newMeter(Dempsy.class, "messages-discarded",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesDispatched = Metrics.newMeter(Dempsy.class, "messages-dispatched",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesFailed = Metrics.newMeter(Dempsy.class, "messages-failed",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesProcessed = Metrics.newMeter(Dempsy.class, "messages-processed",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesSent = Metrics.newMeter(Dempsy.class, "messages-sent",
	         scope, "messages", TimeUnit.SECONDS);
	   messagesUnsent = Metrics.newMeter(Dempsy.class, "messages-unsent",
	         scope, "messsages", TimeUnit.SECONDS);
	   inProcessMessages = new AtomicInteger();
	   messagesInProcess = Metrics.newGauge(Dempsy.class, "messages-in-process",
	         scope, new Gauge<Integer>() {
	      @Override
	      public Integer value()
	      {
	         return inProcessMessages.get();
	      }
	   });
	   
	   numberOfMPs = new AtomicLong();
	   mpsCreated = Metrics.newMeter(Dempsy.class, "message-processors-created",
	         scope, "instances", TimeUnit.SECONDS);
	   mpsDeleted = Metrics.newMeter(Dempsy.class, "message-processors-deleted",
	         scope, "instances", TimeUnit.SECONDS);
	   messageProcessors = Metrics.newGauge(Dempsy.class, "message-processors",
	         scope, new Gauge<Long>() {
	      @Override
	      public Long value() {
	         return numberOfMPs.get();
	      }
	   });
	   
	   preInstantiationDuration = Metrics.newTimer(Dempsy.class, "pre-instantiation-duration",
	         scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

	   outputInvokeDuration = Metrics.newTimer(Dempsy.class, "outputInvoke-duration",
       scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
	   
	   evictionInvokeDuration = Metrics.newTimer(Dempsy.class, "evictionInvoke-duration",
		       scope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
	   
	}
	
	protected MetricsRegistry getMetricsRegistry()
	{
	   return Metrics.defaultRegistry();
	}
	
	@Override
	public void messageReceived(Object message) {
		messagesReceived.mark();
	}
	
	@Override
	public void messageDiscarded(Object message) {
		messagesDiscarded.mark();
		inProcessMessages.decrementAndGet();
	}

	@Override
	public void messageDispatched(Object message) {
		messagesDispatched.mark();
		inProcessMessages.incrementAndGet();
	}

	@Override
	public void messageFailed() {
		messagesFailed.mark();
		inProcessMessages.decrementAndGet();
	}
	
	@Override
	public void messageProcessed(Object message) {
		messagesProcessed.mark();
		inProcessMessages.decrementAndGet();
	}
	
	@Override
	public void messageSent(Object message) {
		messagesSent.mark();
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
	public void messageProcessorDeleted() {
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
		return messagesFailed.count();
	}
	
	@Override
	public long getDiscardedMessageCount()
	{
		return messagesDiscarded.count();
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

	@Override
	public void preInstantiationStarted()
	{
	   preInstantiationDurationContext = preInstantiationDuration.time();
	}
	
	@Override
	public void preInstantiationCompleted()
	{
	   preInstantiationDurationContext.stop();
	}
	
	@Override
	public double getPreInstantiationDuration()
	{
	   return preInstantiationDuration.meanRate();
	}

  @Override
  public void outputInvokeCompleted() {
    outputInvokeDurationContext.stop();
    
  }

  @Override
  public void outputInvokeStarted() {
    outputInvokeDurationContext = outputInvokeDuration.time();
  }
	
  @Override
  public double getOutputInvokeDuration()
  {
     return outputInvokeDuration.meanRate();
  }

	@Override
	public void evictionPassStarted() {
		evictionInvokeDurationContext = evictionInvokeDuration.time();
	}

	@Override
	public void evictionPassCompleted() {
		evictionInvokeDurationContext.stop();
	}

	@Override
	public double getEvictionDuration() {
		return evictionInvokeDuration.meanRate();
	}
}
