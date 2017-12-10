package net.dempsy.monitoring.dummy;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.transport.NodeAddress;

/**
 * Stubbed out Stats Collector implementations that do record no stats. Primarily for testing
 */
public class DummyStatsCollectorFactory implements ClusterStatsCollectorFactory {

    @Override
    public ClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress nodeIdentification) {
        return new DummyClusterStatsCollector();
    }

    @Override
    public void close() throws Exception {}
}
