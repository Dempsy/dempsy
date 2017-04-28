package net.dempsy.utils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

public abstract class MetricUtils {

    public static MetricRegistry getMetricsRegistry() {
        try {
            SharedMetricRegistries.setDefault("default");
        } catch (final Exception e) {}
        return SharedMetricRegistries.getDefault();
    }

}
