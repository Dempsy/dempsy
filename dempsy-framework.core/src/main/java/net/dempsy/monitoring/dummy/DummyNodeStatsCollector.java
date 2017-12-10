package net.dempsy.monitoring.dummy;

import java.util.function.LongSupplier;

import net.dempsy.monitoring.NodeStatsCollector;

/**
 * Stubbed out Stats Collector implementations that do record no stats. Primarily for testing
 */
public class DummyNodeStatsCollector implements NodeStatsCollector {

    @Override
    public void setNodeId(final String nid) {}

    @Override
    public void messageReceived(final Object message) {}

    @Override
    public void messageSent(final Object message) {}

    @Override
    public void messageNotSent() {}

    @Override
    public void messageDiscarded(final Object message) {}

    @Override
    public void setMessagesPendingGauge(final LongSupplier currentMessagesPendingGauge) {}

    @Override
    public void setMessagesOutPendingGauge(final LongSupplier currentMessagesOutPendingGauge) {}

    @Override
    public void stop() {}

}
