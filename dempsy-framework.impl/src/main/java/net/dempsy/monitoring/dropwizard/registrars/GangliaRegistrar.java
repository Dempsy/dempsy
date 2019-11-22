package net.dempsy.monitoring.dropwizard.registrars;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ganglia.GangliaReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;
import net.dempsy.monitoring.dropwizard.DropwizardReporterRegistrar;

public class GangliaRegistrar extends DropwizardReporterRegistrar {
    private static Logger LOGGER = LoggerFactory.getLogger(GangliaRegistrar.class);
    protected String outputDir;

    protected String gangliaHost;
    protected int gangliaPort = -1;

    @Override
    public Reporter registerReporter(final MetricRegistry registry, final String metricPrefix) {
        if(gangliaHost == null)
            throw new IllegalStateException("When using the ganglia reporter, you must configure the ganglia host (gangliaHost property)");
        if(gangliaPort < 0)
            throw new IllegalStateException("When using the ganglia reporter, you must configure the ganglia port (gangliaPort property)");
        if(metricPrefix == null)
            throw new IllegalArgumentException("When using the ganglia reporter, a metric prefix must be supplied.");
        final String gangliaPrefix = metricPrefix.replaceAll("\\.", "-");

        try {
            final GMetric ganglia = new GMetric(gangliaHost, gangliaPort, UDPAddressingMode.MULTICAST, 1);
            final GangliaReporter gangliaReporter = GangliaReporter.forRegistry(registry)
                .prefixedWith(gangliaPrefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(ganglia);
            gangliaReporter.start(period, unit);
            return gangliaReporter;
        } catch(final IOException ioe) {
            LOGGER.warn("Failed to register with Ganglia at {}", gangliaHost + ":" + gangliaPort, ioe);
            return null;
        }
    }

    public String getGangliaHost() {
        return gangliaHost;
    }

    public void setGangliaHost(final String gangliaHost) {
        this.gangliaHost = gangliaHost;
    }

    public int getgangliaPort() {
        return gangliaPort;
    }

    public void setgangliaPort(final int gangliaPort) {
        this.gangliaPort = gangliaPort;
    }

}
