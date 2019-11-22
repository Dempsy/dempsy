package net.dempsy.monitoring.dropwizard;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;

public abstract class DropwizardReporterRegistrar {
    protected long period = 30;
    protected TimeUnit unit = TimeUnit.SECONDS;

    /**
     * Needs to be implemented to return a functioning Reporter. That means that
     * {@link ScheduledReporter}s need to be started before they are returned.
     */
    public abstract Reporter registerReporter(MetricRegistry registry, String metricPrefix);

    public DropwizardReporterRegistrar period(final long period) {
        this.period = period;
        return this;
    }

    public DropwizardReporterRegistrar unit(final TimeUnit unit) {
        this.unit = unit;
        return this;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(final long period) {
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(final TimeUnit unit) {
        this.unit = unit;
    }
}
