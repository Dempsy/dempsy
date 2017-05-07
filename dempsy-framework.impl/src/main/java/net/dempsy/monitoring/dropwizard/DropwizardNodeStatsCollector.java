package net.dempsy.monitoring.dropwizard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.utils.MetricUtils;

public class DropwizardNodeStatsCollector implements NodeStatsCollector {

    public static final String MESSAGE_RECEIVED = "message-received";
    public static final String MESSAGE_DISCARDED = "message-discarded";
    public static final String MESSAGE_SENT = "message-sent";
    public static final String MESSAGE_NOT_SENT = "message-not-sent";
    public static final String MESSAGES_PENDING_GAUGE = "messages-pending-gauge";
    public static final String MESSAGES_OUT_PENDING_GAUGE = "messages-out-pending-gauge";

    private List<DropwizardReporterSpec> reporters = new ArrayList<>();

    private DropwizardStatsReporter reporter;

    private String nodeId;
    private final MetricRegistry registry;
    private final Meter messageReceived;
    private final Meter messageDiscarded;
    private final Meter messageSent;
    private final Meter messageNotSent;

    public DropwizardNodeStatsCollector() {
        registry = MetricUtils.getMetricsRegistry();
        messageReceived = registry.meter(getName(MESSAGE_RECEIVED));
        messageDiscarded = registry.meter(getName(MESSAGE_DISCARDED));
        messageSent = registry.meter(getName(MESSAGE_SENT));
        messageNotSent = registry.meter(getName(MESSAGE_NOT_SENT));
    }

    @Override
    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
        reporter = new DropwizardStatsReporter(this.nodeId, reporters);
    }

    public void setReporters(final List<DropwizardReporterSpec> reporters) {
        this.reporters = reporters;
    }

    @Override
    public void stop() {
        // Stop the reporters
        reporter.stopReporters();
    }

    @Override
    public void messageReceived(final Object message) {
        messageReceived.mark();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messageDiscarded.mark();
    }

    @Override
    public void messageSent(final Object message) {
        messageSent.mark();
    }

    @Override
    public void messageNotSent() {
        messageNotSent.mark();
    }

    @Override
    public void setMessagesPendingGauge(final LongSupplier currentMessagesPendingGauge) {
        final String gaugeName = getName(MESSAGES_PENDING_GAUGE);
        // If the registry doesn't already have this gauge, then add it.
        if (!registry.getGauges().containsKey(gaugeName)) {
            registry.register(gaugeName, new com.codahale.metrics.Gauge<Long>() {
                @Override
                public Long getValue() {
                    return currentMessagesPendingGauge.getAsLong();
                }
            });
        }
    }

    @Override
    public void setMessagesOutPendingGauge(final LongSupplier currentMessagesOutPendingGauge) {
        final String gaugeName = getName(MESSAGES_OUT_PENDING_GAUGE);
        // If the registry doesn't already have this gauge, then add it.
        if (!registry.getGauges().containsKey(gaugeName)) {
            registry.register(gaugeName, new com.codahale.metrics.Gauge<Long>() {
                @Override
                public Long getValue() {
                    return currentMessagesOutPendingGauge.getAsLong();
                }
            });
        }
    }

    // protected access for testing purposes
    protected String getName(final String key) {
        return MetricRegistry.name(DropwizardNodeStatsCollector.class, "node", key);
    }

}
