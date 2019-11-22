package net.dempsy.monitoring.dropwizard.registrars;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import net.dempsy.monitoring.dropwizard.DropwizardReporterRegistrar;

public class ConsoleRegistrar extends DropwizardReporterRegistrar {

    @Override
    public Reporter registerReporter(final MetricRegistry registry, final String metricsPrefix) {
        final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        consoleReporter.start(period, unit);
        return consoleReporter;
    }

}
