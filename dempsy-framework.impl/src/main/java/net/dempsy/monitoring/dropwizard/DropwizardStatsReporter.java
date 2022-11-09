package net.dempsy.monitoring.dropwizard;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SafeString;
import net.dempsy.utils.MetricUtils;

public class DropwizardStatsReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardStatsReporter.class);

    private final List<Reporter> reporters = Collections.synchronizedList(new LinkedList<>());

    /**
     * Creates reporters from the given reporter specifications.
     *
     * @param metricPrefix
     *     A prefix applied to each metric. This is only applicable for the Graphite and Graphana reporters, and is required for those.
     * @param reporterSpecs
     *     A list of specifications defining which reporters should be created and their configurations.
     */
    public DropwizardStatsReporter(final String metricPrefix, final List<DropwizardReporterRegistrar> reporterSpecs) {
        for(final DropwizardReporterRegistrar spec: reporterSpecs) {
            final MetricRegistry metrics = MetricUtils.getMetricsRegistry();
            final Reporter reporter = spec.registerReporter(metrics, metricPrefix);
            if(reporter != null)
                reporters.add(reporter);
            else
                LOGGER.warn("Reporter registrar \"{}\" returned a null reporter", SafeString.objectDescription(spec));
        }

    }

    public void stopReporters() {
        reporters.stream()
            .forEach(r -> {
                try {
                    r.close();
                } catch(final IOException ioe) {
                    LOGGER.error("Close of " + SafeString.objectDescription(r), ioe);
                }
            });
    }

}
