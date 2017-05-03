package net.dempsy.monitoring.dropwizard;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.StatsCollector.TimerContext;

public class TestDropwizardClusterStatsCollector {

    private DropwizardClusterStatsCollector collector;

    @Before
    public void createCollector() {
        collector = new DropwizardClusterStatsCollector(new ClusterId("appName", "clusterName"));
    }

    @Test
    public void verifyGetNameHasNecessaryPieces() {
        // We don't really care if something minor changes (like the ordering of output fields or package name), but we do want to make sure
        // this still contains all the important bits.
        final String name = collector.getName("metric-name-goes-here");
        Assert.assertTrue(name.contains("appName"));
        Assert.assertTrue(name.contains("clusterName"));
        Assert.assertTrue(name.contains("metric-name-goes-here"));
    }

    @Test
    public void verifyMetersAreCreated() {
        // Dispatch some messages. The parameters don't matter for the metrics we're collecting.
        collector.messageDispatched(null);
        collector.messageProcessed(null);
        collector.messageFailed(true);
        collector.messageCollision(null);
        collector.messageProcessorCreated(null);
        collector.messageProcessorDeleted(null);

        // Verify the metrics were created and have the correct values
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_DISPATCHED, 1);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_PROCESSED, 1);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_FAILED, 1);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_COLLISION, 1);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_PROCESSOR_CREATED, 1);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_PROCESSOR_DELETED, 1);

        // Lets call this one a couple more times to make sure the meter increments correctly.
        collector.messageDispatched(null);
        collector.messageDispatched(null);
        verifyMeter(DropwizardClusterStatsCollector.MESSAGES_DISPATCHED, 3);
    }

    private void verifyMeter(final String key, final long expectedValue) {
        final Meter m = SharedMetricRegistries.getDefault().getMeters().get(collector.getName(key));
        Assert.assertNotNull(m);
        Assert.assertEquals(expectedValue, m.getCount());
    }

    @Test
    public void verifyTimerContexts() throws InterruptedException {
        verifyTimer(collector.preInstantiationStarted(), DropwizardClusterStatsCollector.PRE_INSTANTIATION_STARTED_TIMER);
        verifyTimer(collector.outputInvokeStarted(), DropwizardClusterStatsCollector.OUTPUT_INVOKE_STARTED_TIMER);
        verifyTimer(collector.evictionPassStarted(), DropwizardClusterStatsCollector.EVICTION_PASS_STARTED_TIMER);
    }

    private void verifyTimer(final TimerContext tc, final String key) throws InterruptedException {
        // Make sure the timer is registered as a metric
        final Timer t = SharedMetricRegistries.getDefault().getTimers().get(collector.getName(key));
        Assert.assertNotNull(t);

        // Stop the timer and verify the time elapsed isn't zero
        tc.close();
        Assert.assertEquals(1, t.getCount());
        Assert.assertTrue(t.getSnapshot().getMax() > 0);
        Assert.assertEquals(t.getSnapshot().getMax(), t.getSnapshot().getMin());
    }

}
