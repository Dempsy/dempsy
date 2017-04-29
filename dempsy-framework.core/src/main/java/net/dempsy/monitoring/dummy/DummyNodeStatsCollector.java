package net.dempsy.monitoring.dummy;

import net.dempsy.monitoring.NodeStatsCollector;

public class DummyNodeStatsCollector implements NodeStatsCollector {

    @Override
    public void messageReceived(final Object message) {}

    @Override
    public void messageSent(final Object message) {}

    @Override
    public void messageNotSent() {}

    @Override
    public void messageDiscarded(final Object message) {}

    @Override
    public void setMessagesPendingGauge(final Gauge currentMessagesPendingGauge) {}

    @Override
    public void setMessagesOutPendingGauge(final Gauge currentMessagesOutPendingGauge) {}

    @Override
    public void stop() {}

}
