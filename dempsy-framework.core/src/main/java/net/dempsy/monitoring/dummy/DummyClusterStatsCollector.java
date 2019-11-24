package net.dempsy.monitoring.dummy;

import net.dempsy.monitoring.ClusterStatsCollector;

/**
 * Stubbed out Stats Collector implementations that do record no stats. Primarily for testing
 */
public class DummyClusterStatsCollector implements ClusterStatsCollector {

    @Override
    public void messageDispatched(final int num) {}

    @Override
    public void messageProcessed(final int num) {}

    @Override
    public void messageFailed(final int num) {}

    @Override
    public void messageCollision(final Object message) {}

    @Override
    public void messageDiscarded(final Object message) {}

    @Override
    public void messageProcessorCreated(final Object key) {}

    @Override
    public void messageProcessorDeleted(final Object key) {}

    @Override
    public void stop() {}

    @Override
    public TimerContext preInstantiationStarted() {
        return () -> {};
    }

    @Override
    public TimerContext outputInvokeStarted() {
        return () -> {};
    }

    @Override
    public TimerContext evictionPassStarted() {
        return () -> {};
    }

}
