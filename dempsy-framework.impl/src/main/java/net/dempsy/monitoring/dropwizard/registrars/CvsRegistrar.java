package net.dempsy.monitoring.dropwizard.registrars;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import net.dempsy.monitoring.dropwizard.DropwizardReporterRegistrar;

public class CvsRegistrar extends DropwizardReporterRegistrar {
    protected String outputDir;

    @Override
    public Reporter registerReporter(final MetricRegistry registry, final String metricsPrefix) {
        final CsvReporter csvReporter = CsvReporter.forRegistry(registry)
            .formatFor(Locale.US)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(new File(outputDir));
        csvReporter.start(period, unit);
        return csvReporter;
    }

    public CvsRegistrar outputDir(final String outputDir) {
        this.outputDir = outputDir;
        return this;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(final String outputDir) {
        this.outputDir = outputDir;
    }

}
