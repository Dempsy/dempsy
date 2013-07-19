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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class TestStatsCollectorCoda {

   StatsCollectorCoda stats;
   ClusterId clusterId = new ClusterId("appliction", "cluster");

   @SuppressWarnings("unchecked")
   long getStatValue(StatsCollectorCoda statCollector, String metricName)
   {
      MetricsRegistry metricReg = statCollector.getMetricsRegistry();
      Object meter = metricReg.allMetrics().get(statCollector.createName(metricName));
      if (com.yammer.metrics.core.Gauge.class.isAssignableFrom(meter.getClass()))
         return ((com.yammer.metrics.core.Gauge<Long>)metricReg.allMetrics().get(statCollector.createName(metricName))).value();
      else if (com.yammer.metrics.core.Histogram.class.isAssignableFrom(meter.getClass()))
      {
         final com.yammer.metrics.core.Histogram h = (com.yammer.metrics.core.Histogram)metricReg.allMetrics().get(statCollector.createName(metricName));
         return Math.round(h.count() * h.mean());
      }
      else
         return ((Metered)metricReg.allMetrics().get(statCollector.createName(metricName))).count();
   }

   private StatsCollectorFactoryCoda statsCollectorFactory;

   @Before
   public void setUp() throws Exception {
      statsCollectorFactory = new StatsCollectorFactoryCoda();
      stats = new StatsCollectorCoda(clusterId,statsCollectorFactory.getNamingStrategy());
   }

   @After
   public void tearDown() throws Throwable
   {
      if (stats != null)
      {
         stats.stop();
         stats = null;
      }
   }

   @Test
   public void testEnvPrefixFromSystemProperty() throws Throwable
   {
      tearDown();
      System.setProperty(StatsCollectorFactoryCoda.environmentPrefixSystemPropertyName,"Hello.");
      setUp();
      StatsCollectorFactoryCoda.MetricNamingStrategy strategy = statsCollectorFactory.getNamingStrategy();
      ClusterId clusterId = new ClusterId("app","cluster");
      MetricName name = strategy.createName(clusterId, "metric");

      assertEquals("metric", name.getName());
      assertEquals("app-cluster", name.getGroup());
      assertTrue(strategy.buildPrefix(clusterId, new Destination() { @Override public String toString() { return "destination"; } }).startsWith("Hello."));

      // make sure setting the environment prefix doesn't effect the -D option
      statsCollectorFactory.setEnvironmentPrefix("otherEnvPrefix");
      assertTrue(strategy.buildPrefix(clusterId, new Destination() { @Override public String toString() { return "destination"; } }).startsWith("Hello."));

      // make sure that without the system property the setEnvironmentPrefix value works
      System.getProperties().remove(StatsCollectorFactoryCoda.environmentPrefixSystemPropertyName);
      assertTrue(strategy.buildPrefix(clusterId, new Destination() { @Override public String toString() { return "destination"; } }).startsWith("otherEnvPrefix"));

      // make sure that delting the environmentPrefix doesn't create an issue.
      statsCollectorFactory.setEnvironmentPrefix(null);
      strategy.buildPrefix(clusterId, new Destination() { @Override public String toString() { return "destination"; } }).startsWith("otherEnvPrefix");
   }

   @Test
   public void testMessageReceived() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_RCVD));
      stats.messageReceived(new byte[3]);
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_RCVD));
      assertEquals("got one", 3L, getStatValue(stats, StatsCollectorCoda.MN_BYTES_RCVD));
   }

   @Test
   public void testMessageDiscarded() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_DISCARD));
      stats.messageDiscarded("foo");
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_DISCARD));
   }

   @Test
   public void testMessageCollisions() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_COLLISION));
      stats.messageCollision("foo");
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_COLLISION));
   }

   @Test
   public void testMessageDispatched() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_DISPATCH));
      stats.messageDispatched("foo");
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_DISPATCH));
   }

   @Test
   public void testMessageFailed() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_FWFAIL));
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_MPFAIL));
      stats.messageFailed(true);
      assertEquals("got one", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_FWFAIL));
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_MPFAIL));
      stats.messageFailed(false);
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_FWFAIL));
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_MPFAIL));
   }

   @Test
   public void testMessageProcessed() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_PROC));
      stats.messageProcessed("foo");
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_PROC));
   }

   @Test
   public void testMessageSent() {
      assertEquals("no messages yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MSG_SENT));
      stats.messageSent(new byte[3]);
      assertEquals("got one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MSG_SENT));
      assertEquals("got one", 3L, getStatValue(stats, StatsCollectorCoda.MN_BYTES_SENT));
   }

   @Test
   public void testMessageProcessorCreated() {
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MP_CREATE));
      stats.messageProcessorCreated("abc");
      assertEquals("made one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MP_CREATE));
   }

   @Test
   public void testMessageProcessorDeleted() 
   {
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MP_DELETE));
      stats.messageProcessorDeleted("abc");
      assertEquals("del one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MP_DELETE));
   }

   @Test
   public void testMessagesPending() {
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_PENDING));
      assertEquals("none yet", 0L, stats.getMessagesPending());
      final AtomicLong count = new AtomicLong(0);
      StatsCollector.Gauge gauge = new StatsCollector.Gauge()
      {
         @Override
         public long value()
         {
            return count.get();
         }
      };
      stats.setMessagesPendingGauge(gauge);
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_PENDING));
      assertEquals("none yet", 0L, stats.getMessagesPending());
      count.set(10);
      assertEquals("final value", 10L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_PENDING));
      assertEquals("final value", 10L, stats.getMessagesPending());
   }

   @Test
   public void testMessagesOutPending() {
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_OUT_PENDING));
      assertEquals("none yet", 0L, stats.getMessagesPending());
      final AtomicLong count = new AtomicLong(0);
      StatsCollector.Gauge gauge = new StatsCollector.Gauge()
      {
         @Override
         public long value()
         {
            return count.get();
         }
      };
      stats.setMessagesOutPendingGauge(gauge);
      assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_OUT_PENDING));
      assertEquals("none yet", 0L, stats.getMessagesOutPending());
      count.set(10);
      assertEquals("final value", 10L, getStatValue(stats, StatsCollectorCoda.GAGE_MSG_OUT_PENDING));
      assertEquals("final value", 10L, stats.getMessagesOutPending());
   }
}
