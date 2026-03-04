package net.dempsy.monitoring.micrometer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.StatsCollector.TimerContext;

public class TestMicrometerClusterStatsCollector {

    private SimpleMeterRegistry registry;
    private MicrometerClusterStatsCollector collector;

    @Before
    public void createCollector() {
        registry = new SimpleMeterRegistry();
        collector = new MicrometerClusterStatsCollector(new ClusterId("appName", "clusterName"), registry);
    }

    @After
    public void cleanup() {
        registry.close();
    }

    @Test
    public void verifyCountersAreCreated() {
        collector.messageDispatched(1);
        collector.messageProcessed(1);
        collector.messageFailed(1);
        collector.messageCollision(null);
        collector.messageProcessorCreated(null);
        collector.messageProcessorDeleted(null);

        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_DISPATCHED, 1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_PROCESSED, 1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_FAILED, 1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_COLLISION, 1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_PROCESSOR_CREATED, 1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_PROCESSOR_DELETED, 1);

        collector.messageDispatched(1);
        collector.messageDispatched(1);
        verifyCounter(MicrometerClusterStatsCollector.MESSAGES_DISPATCHED, 3);
    }

    @Test
    public void verifyMetricsGetCleanedUp() {
        collector.messageDispatched(1);
        collector.messageProcessed(1);
        collector.messageFailed(1);
        collector.messageCollision(null);
        collector.messageProcessorCreated(null);
        collector.messageProcessorDeleted(null);

        collector.outputInvokeStarted();
        collector.evictionPassStarted();
        collector.preInstantiationStarted();

        Assert.assertFalse(registry.getMeters().isEmpty());

        collector.close();

        Assert.assertTrue(registry.getMeters().isEmpty());
    }

    private void verifyCounter(final String name, final long expectedValue) {
        final Counter c = registry.find(name)
            .tag("app", "appName")
            .tag("cluster", "clusterName")
            .counter();
        Assert.assertNotNull("Counter " + name + " not found", c);
        Assert.assertEquals(expectedValue, (long)c.count());
    }

    @Test
    public void verifyTimerContexts() {
        verifyTimer(collector.preInstantiationStarted(), MicrometerClusterStatsCollector.PRE_INSTANTIATION_TIMER);
        verifyTimer(collector.outputInvokeStarted(), MicrometerClusterStatsCollector.OUTPUT_INVOKE_TIMER);
        verifyTimer(collector.evictionPassStarted(), MicrometerClusterStatsCollector.EVICTION_PASS_TIMER);
    }

    private void verifyTimer(final TimerContext tc, final String name) {
        tc.close();

        final Timer t = registry.find(name)
            .tag("app", "appName")
            .tag("cluster", "clusterName")
            .timer();
        Assert.assertNotNull("Timer " + name + " not found", t);
        Assert.assertEquals(1, t.count());
        Assert.assertTrue(t.max(java.util.concurrent.TimeUnit.NANOSECONDS) > 0);
    }
}
