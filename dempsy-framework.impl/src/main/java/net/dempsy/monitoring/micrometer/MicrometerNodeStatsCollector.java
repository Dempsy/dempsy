package net.dempsy.monitoring.micrometer;

import java.util.function.LongSupplier;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import net.dempsy.monitoring.NodeStatsCollector;

public class MicrometerNodeStatsCollector implements NodeStatsCollector {

    public static final String MESSAGE_RECEIVED = "dempsy.node.messages.received";
    public static final String MESSAGE_DISCARDED = "dempsy.node.messages.discarded";
    public static final String MESSAGE_SENT = "dempsy.node.messages.sent";
    public static final String MESSAGE_NOT_SENT = "dempsy.node.messages.not.sent";
    public static final String MESSAGES_PENDING_GAUGE = "dempsy.node.messages.pending";
    public static final String MESSAGES_OUT_PENDING_GAUGE = "dempsy.node.messages.out.pending";

    private final MeterRegistry registry;
    private final Counter messageReceived;
    private final Counter messageDiscarded;
    private final Counter messageSent;
    private final Counter messageNotSent;

    private final AtomicLong messagesPending = new AtomicLong();
    private final AtomicLong messagesOutPending = new AtomicLong();
    private boolean pendingGaugeRegistered = false;
    private boolean outPendingGaugeRegistered = false;

    public MicrometerNodeStatsCollector() {
        this(io.micrometer.core.instrument.Metrics.globalRegistry);
    }

    public MicrometerNodeStatsCollector(final MeterRegistry registry) {
        this.registry = registry;
        messageReceived = Counter.builder(MESSAGE_RECEIVED).register(registry);
        messageDiscarded = Counter.builder(MESSAGE_DISCARDED).register(registry);
        messageSent = Counter.builder(MESSAGE_SENT).register(registry);
        messageNotSent = Counter.builder(MESSAGE_NOT_SENT).register(registry);
    }

    @Override
    public void setNodeId(final String nodeId) {
        // Node ID available for tagging if needed in the future
    }

    @Override
    public void stop() {
        registry.getMeters().stream()
            .filter(m -> m.getId().getName().startsWith("dempsy.node."))
            .forEach(registry::remove);
    }

    @Override
    public void messageReceived(final Object message) {
        messageReceived.increment();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messageDiscarded.increment();
    }

    @Override
    public void messageSent(final Object message) {
        messageSent.increment();
    }

    @Override
    public void messageNotSent() {
        messageNotSent.increment();
    }

    @Override
    public void setMessagesPendingGauge(final LongSupplier currentMessagesPendingGauge) {
        if(!pendingGaugeRegistered) {
            Gauge.builder(MESSAGES_PENDING_GAUGE, currentMessagesPendingGauge, LongSupplier::getAsLong)
                .register(registry);
            pendingGaugeRegistered = true;
        }
    }

    @Override
    public void setMessagesOutPendingGauge(final LongSupplier currentMessagesOutPendingGauge) {
        if(!outPendingGaugeRegistered) {
            Gauge.builder(MESSAGES_OUT_PENDING_GAUGE, currentMessagesOutPendingGauge, LongSupplier::getAsLong)
                .register(registry);
            outPendingGaugeRegistered = true;
        }
    }

    MeterRegistry getRegistry() {
        return registry;
    }
}
