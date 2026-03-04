package net.dempsy.monitoring.micrometer;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.transport.NodeAddress;

public class MicrometerStatsCollectorFactory implements ClusterStatsCollectorFactory {
    private final Map<ClusterId, MicrometerClusterStatsCollector> collectors = new HashMap<>();
    private final MeterRegistry registry;

    public MicrometerStatsCollectorFactory() {
        this(Metrics.globalRegistry);
    }

    public MicrometerStatsCollectorFactory(final MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public synchronized ClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress nodeIdentification) {
        MicrometerClusterStatsCollector ret = collectors.get(clusterId);
        if(ret == null) {
            ret = new MicrometerClusterStatsCollector(clusterId, registry);
            collectors.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public synchronized void close() throws Exception {
        for(final Map.Entry<ClusterId, MicrometerClusterStatsCollector> e: collectors.entrySet()) {
            e.getValue().close();
        }
        collectors.clear();
    }
}
