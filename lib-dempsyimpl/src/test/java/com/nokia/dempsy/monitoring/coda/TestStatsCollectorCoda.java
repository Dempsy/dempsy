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

import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.config.ClusterId;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class TestStatsCollectorCoda {
		
	StatsCollectorCoda stats;
	ClusterId clusterId = new ClusterId("appliction", "cluster");
	String scope = clusterId.getApplicationName() + "." + clusterId.getMpClusterName();
	
	long getStatValue(StatsCollectorCoda statCollector, String metricName)
	{
		MetricsRegistry metricReg = statCollector.getMetricsRegistry();
		Metered meter = (Metered)metricReg.allMetrics().get(new MetricName(Dempsy.class, metricName, scope));
		return meter.count();
	}
	

	@Before
	public void setUp() throws Exception {
		stats = new StatsCollectorCoda(clusterId);
		
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
	public void testMessageProcessorDeleted() {
		assertEquals("none yet", 0L, getStatValue(stats, StatsCollectorCoda.MN_MP_DELETE));
		stats.messageProcessorDeleted("abc");
		assertEquals("del one", 1L, getStatValue(stats, StatsCollectorCoda.MN_MP_DELETE));
	}

}
