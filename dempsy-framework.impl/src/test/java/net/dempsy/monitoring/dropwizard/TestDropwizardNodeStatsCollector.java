package net.dempsy.monitoring.dropwizard;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;

import net.dempsy.cluster.ClusterInfoException;

public class TestDropwizardNodeStatsCollector {

    private DropwizardNodeStatsCollector collector;

    @Before
    public void createCollector() throws ClusterInfoException {
        collector = new DropwizardNodeStatsCollector();
        collector.setNodeId("nodeId");
    }

    @Test
    public void verifyGetNameHasNecessaryPieces() {
        // We don't really care if something minor changes (like the ordering of output fields or package name), but we do want to make sure
        // this still contains all the important bits.
        final String name = collector.getName("metric-name-goes-here");
        Assert.assertTrue(name.contains("metric-name-goes-here"));
    }

    @Test
    public void verifyMetersAreCreated() {
        // Dispatch some messages. The parameters don't matter for the metrics we're collecting.
        collector.messageReceived(null);
        collector.messageDiscarded(null);
        collector.messageSent(null);
        collector.messageNotSent();

        // Verify the metrics were created and have the correct values
        verifyMeter(DropwizardNodeStatsCollector.MESSAGE_RECEIVED, 1);
        verifyMeter(DropwizardNodeStatsCollector.MESSAGE_DISCARDED, 1);
        verifyMeter(DropwizardNodeStatsCollector.MESSAGE_SENT, 1);
        verifyMeter(DropwizardNodeStatsCollector.MESSAGE_NOT_SENT, 1);

        // Lets call this one a couple more times to make sure the meter increments correctly.
        collector.messageReceived(null);
        collector.messageReceived(null);
        verifyMeter(DropwizardNodeStatsCollector.MESSAGE_RECEIVED, 3);
    }

    private void verifyMeter(final String key, final long expectedValue) {
        final Meter m = SharedMetricRegistries.getDefault().getMeters().get(collector.getName(key));
        Assert.assertNotNull(m);
        Assert.assertEquals(expectedValue, m.getCount());
    }

    @Test
    public void verifyGaugesGetSet() {
        collector.setMessagesPendingGauge(() -> 1L);
        collector.setMessagesOutPendingGauge(() -> 2L);

        verifyGauge(DropwizardNodeStatsCollector.MESSAGES_PENDING_GAUGE, 1);
        verifyGauge(DropwizardNodeStatsCollector.MESSAGES_OUT_PENDING_GAUGE, 2);
    }

    private void verifyGauge(final String key, final long expectedValue) {
        @SuppressWarnings("unchecked")
        final Gauge<Long> g = SharedMetricRegistries.getDefault().getGauges().get(collector.getName(key));
        Assert.assertNotNull(g);
        Assert.assertEquals(Long.valueOf(expectedValue), g.getValue());
    }

}
