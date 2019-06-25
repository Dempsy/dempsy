package net.dempsy.monitoring.dropwizard;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Value Object to hold parameters for various Metrics Reporter's setup arguments.
 */
public class DropwizardReporterSpec {

    private DropwizardReporterType type;
    private TimeUnit unit;
    private long period;
    private String hostName;
    private int portNumber;
    private File outputDir;

    /**
     * Set the type of report to issue.
     */
    public void setType(final DropwizardReporterType type) {
        this.type = type;
    }

    /**
     * Get the type of report to issue
     */
    public DropwizardReporterType getType() {
        return this.type;
    }

    /**
     * Set the time unit to measure the period in
     *
     * @see TimeUnit
     */
    public void setUnit(final TimeUnit unit) {
        this.unit = unit;
    }

    /**
     * Get the time unit in which the period is measured in
     */
    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * Set the period to reports statistics in. In conjunction with the TimeUnit set, controls how often statistics are published.
     */
    public void setPeriod(final long period) {
        this.period = period;
    }

    /**
     * Get the period in which statistics are reported
     */
    public long getPeriod() {
        return period;
    }

    /**
     * Set the hostname to publish statistics to. This is only useful for the Graphite and Ganglia Reporters
     */
    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the host name statistics are published to
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Set the port statistics are published to. Like the hostname, this is only useful for network enabled reporters, i.e. Graphite and Ganglia.
     */
    public void setPortNumber(final int portNumber) {
        this.portNumber = portNumber;
    }

    /**
     * Get the port statistics are published to
     */
    public int getPortNumber() {
        return portNumber;
    }

    /**
     * Set the directory statistics are published to. Only useful for the CSV reporter that writes files there.
     */
    public void setOutputDir(final File outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * Get the directory statistics are published to.
     */
    public File getOutputDir() {
        return outputDir;
    }
}
