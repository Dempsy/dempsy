package net.dempsy.monitoring.dropwizard.registrars;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import net.dempsy.monitoring.dropwizard.DropwizardReporterRegistrar;

public class GraphiteRegistrar extends DropwizardReporterRegistrar {
    protected String outputDir;

    protected String graphiteHostname;
    protected int graphitePort = -1;

    @Override
    public Reporter registerReporter(final MetricRegistry registry, final String metricPrefix) {
        if(graphiteHostname == null)
            throw new IllegalStateException("When using the graphite reporter, you must configure the graphite host (graphiteHostname property)");
        if(graphitePort < 0)
            throw new IllegalStateException("When using the graphite reporter, you must configure the graphite port (graphitePort property)");
        if(metricPrefix == null)
            throw new IllegalArgumentException("When using the graphite reporter, a metric prefix must be supplied.");
        final String graphitePrefix = metricPrefix.replaceAll("\\.", "-");

        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostname, graphitePort));
        final GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(registry)
            .prefixedWith(graphitePrefix)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(graphite);
        graphiteReporter.start(period, unit);
        return graphiteReporter;
    }

    public String getGraphiteHostname() {
        return graphiteHostname;
    }

    public void setGraphiteHostname(final String graphiteHostname) {
        this.graphiteHostname = graphiteHostname;
    }

    public int getGraphitePort() {
        return graphitePort;
    }

    public void setGraphitePort(final int graphitePort) {
        this.graphitePort = graphitePort;
    }

}
