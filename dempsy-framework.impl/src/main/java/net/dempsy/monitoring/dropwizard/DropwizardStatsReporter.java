package net.dempsy.monitoring.dropwizard;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;
import net.dempsy.utils.MetricUtils;

public class DropwizardStatsReporter {
    public static final Logger LOGGER = LoggerFactory.getLogger(DropwizardStatsReporter.class);

    private final List<ScheduledReporter> reporters = Collections.synchronizedList(new LinkedList<>());

    /**
     * Creates reporters from the given reporter specifications.
     * 
     * @param metricPrefix
     *            A prefix applied to each metric. This is only applicable for the Graphite and Graphana reporters, and is required for those.
     * @param reporterSpecs
     *            A list of specifications defining which reporters should be created and their configurations.
     */
    public DropwizardStatsReporter(final String metricPrefix, final List<DropwizardReporterSpec> reporterSpecs) {
        for (final DropwizardReporterSpec spec : reporterSpecs) {
            try {
                final MetricRegistry metrics = MetricUtils.getMetricsRegistry();

                switch (spec.getType()) {
                    case CONSOLE:
                        final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build();
                        consoleReporter.start(spec.getPeriod(), spec.getUnit());
                        reporters.add(consoleReporter);
                        break;

                    case CSV:
                        final CsvReporter csvReporter = CsvReporter.forRegistry(metrics)
                                .formatFor(Locale.US)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build(spec.getOutputDir());
                        csvReporter.start(spec.getPeriod(), spec.getUnit());
                        reporters.add(csvReporter);
                        break;

                    case GRAPHITE:
                        if (metricPrefix == null)
                            throw new IllegalArgumentException("When using the graphite reporter, a metric prefix must be supplied.");
                        final String graphitePrefix = metricPrefix.replaceAll("\\.", "-");

                        final Graphite graphite = new Graphite(new InetSocketAddress(spec.getHostName(), spec.getPortNumber()));
                        final GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(metrics)
                                .prefixedWith(graphitePrefix)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build(graphite);
                        graphiteReporter.start(spec.getPeriod(), spec.getUnit());
                        reporters.add(graphiteReporter);
                        break;

                    case GANGLIA:
                        if (metricPrefix == null)
                            throw new IllegalArgumentException("When using the ganglia reporter, a metric prefix must be supplied.");
                        final String gangliaPrefix = metricPrefix.replaceAll("\\.", "-");

                        final GMetric ganglia = new GMetric(spec.getHostName(), spec.getPortNumber(), UDPAddressingMode.MULTICAST, 1);
                        final GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics)
                                .prefixedWith(gangliaPrefix)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build(ganglia);
                        gangliaReporter.start(spec.getPeriod(), spec.getUnit());
                        reporters.add(gangliaReporter);
                        break;
                }
            } catch (final Exception e) {
                LOGGER.error("Can't initialize Metrics Reporter " + spec.toString(), e);
            }
        }
    }

    public void stopReporters() {
        reporters.forEach(r -> r.stop());
    }

}
