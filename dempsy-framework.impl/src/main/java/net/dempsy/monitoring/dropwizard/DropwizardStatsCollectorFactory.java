package net.dempsy.monitoring.dropwizard;

import java.util.HashMap;
import java.util.Map;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.transport.NodeAddress;

public class DropwizardStatsCollectorFactory implements ClusterStatsCollectorFactory {
    Map<ClusterId, DropwizardClusterStatsCollector> collectors = new HashMap<>();

    @Override
    public ClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress nodeIdentification) {
        DropwizardClusterStatsCollector ret = collectors.get(clusterId);
        if (ret == null) {
            ret = new DropwizardClusterStatsCollector(clusterId);
            collectors.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public synchronized void close() throws Exception {
        for (final Map.Entry<ClusterId, DropwizardClusterStatsCollector> e : collectors.entrySet()) {
            e.getValue().close();
        }
        collectors.clear();
    }

}
