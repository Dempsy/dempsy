package net.dempsy.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.StatsCollector.TimerContext;

public class TestMicrometerClusterStatsCollector {

    private SimpleMeterRegistry registry;
    private MicrometerClusterStatsCollector collector;

    @BeforeEach
    public void createCollector() {
        registry = new SimpleMeterRegistry();
        collector = new MicrometerClusterStatsCollector(new ClusterId("appName", "clusterName"), registry);
    }

    @AfterEach
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

        assertFalse(registry.getMeters().isEmpty());

        collector.close();

        assertTrue(registry.getMeters().isEmpty());
    }

    private void verifyCounter(final String name, final long expectedValue) {
        final Counter c = registry.find(name)
            .tag("app", "appName")
            .tag("cluster", "clusterName")
            .counter();
        assertNotNull(c, "Counter " + name + " not found");
        assertEquals(expectedValue, (long)c.count());
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
        assertNotNull(t, "Timer " + name + " not found");
        assertEquals(1, t.count());
        assertTrue(t.max(java.util.concurrent.TimeUnit.NANOSECONDS) > 0);
    }
}
