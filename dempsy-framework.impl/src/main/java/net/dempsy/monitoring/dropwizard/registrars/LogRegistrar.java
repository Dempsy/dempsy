package net.dempsy.monitoring.dropwizard.registrars;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Slf4jReporter.Builder;
import com.codahale.metrics.Slf4jReporter.LoggingLevel;

import net.dempsy.monitoring.dropwizard.DropwizardReporterRegistrar;

public class LogRegistrar extends DropwizardReporterRegistrar {

    protected String prefix = null;
    protected LoggingLevel loggingLevel = LoggingLevel.INFO;

    @Override
    public Reporter registerReporter(final MetricRegistry registry, final String metricPrefix) {
        final Builder builder = Slf4jReporter.forRegistry(registry)
            .shutdownExecutorOnStop(true)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .withLoggingLevel(loggingLevel)

        ;

        if(prefix != null)
            builder.prefixedWith(prefix);

        final Slf4jReporter reporter = builder.build();
        reporter.start(period, unit);
        return reporter;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(final String prefix) {
        this.prefix = prefix;
    }

    public LoggingLevel getLoggingLevel() {
        return loggingLevel;
    }

    public void setLoggingLevel(final LoggingLevel loggingLevel) {
        this.loggingLevel = loggingLevel;
    }
}
