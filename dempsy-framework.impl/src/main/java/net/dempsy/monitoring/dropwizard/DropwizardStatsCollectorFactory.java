package net.dempsy.monitoring.dropwizard;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.transport.NodeAddress;

public class DropwizardStatsCollectorFactory implements ClusterStatsCollectorFactory {

    @Override
    public ClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress nodeIdentification) {
        return new DropwizardClusterStatsCollector(clusterId);
    }

    @Override
    public void close() throws Exception {}

}
