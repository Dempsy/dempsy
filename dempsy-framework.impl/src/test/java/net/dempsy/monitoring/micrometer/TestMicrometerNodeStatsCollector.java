package net.dempsy.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import net.dempsy.cluster.ClusterInfoException;

public class TestMicrometerNodeStatsCollector {

    private SimpleMeterRegistry registry;
    private MicrometerNodeStatsCollector collector;

    @BeforeEach
    public void createCollector() throws ClusterInfoException {
        registry = new SimpleMeterRegistry();
        collector = new MicrometerNodeStatsCollector(registry);
        collector.setNodeId("nodeId");
    }

    @AfterEach
    public void cleanup() {
        registry.close();
    }

    @Test
    public void verifyCountersAreCreated() {
        collector.messageReceived(null);
        collector.messageDiscarded(null);
        collector.messageSent(null);
        collector.messageNotSent();

        verifyCounter(MicrometerNodeStatsCollector.MESSAGE_RECEIVED, 1);
        verifyCounter(MicrometerNodeStatsCollector.MESSAGE_DISCARDED, 1);
        verifyCounter(MicrometerNodeStatsCollector.MESSAGE_SENT, 1);
        verifyCounter(MicrometerNodeStatsCollector.MESSAGE_NOT_SENT, 1);

        collector.messageReceived(null);
        collector.messageReceived(null);
        verifyCounter(MicrometerNodeStatsCollector.MESSAGE_RECEIVED, 3);
    }

    private void verifyCounter(final String name, final long expectedValue) {
        final Counter c = registry.find(name).counter();
        assertNotNull(c, "Counter " + name + " not found");
        assertEquals(expectedValue, (long)c.count());
    }

    @Test
    public void verifyGaugesGetSet() {
        collector.setMessagesPendingGauge(() -> 1L);
        collector.setMessagesOutPendingGauge(() -> 2L);

        verifyGauge(MicrometerNodeStatsCollector.MESSAGES_PENDING_GAUGE, 1);
        verifyGauge(MicrometerNodeStatsCollector.MESSAGES_OUT_PENDING_GAUGE, 2);
    }

    private void verifyGauge(final String name, final long expectedValue) {
        final Gauge g = registry.find(name).gauge();
        assertNotNull(g, "Gauge " + name + " not found");
        assertEquals((double)expectedValue, g.value(), 0.001);
    }

    @Test
    public void verifyMetricsGetCleanedUp() {
        collector.messageReceived(null);
        collector.messageDiscarded(null);
        collector.messageSent(null);
        collector.messageNotSent();

        collector.setMessagesPendingGauge(() -> 1L);
        collector.setMessagesOutPendingGauge(() -> 2L);

        assertFalse(registry.getMeters().isEmpty());

        collector.close();

        assertTrue(registry.getMeters().isEmpty());
    }
}
