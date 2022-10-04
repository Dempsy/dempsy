package net.dempsy.monitoring.dropwizard;

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.utils.MetricUtils;

public class DropwizardClusterStatsCollector implements ClusterStatsCollector {

    public static final String MESSAGES_DISPATCHED = "messages-dispatched";
    public static final String MESSAGES_PROCESSED = "messages-processed";
    public static final String MESSAGES_FAILED = "messages-failed";
    public static final String MESSAGES_COLLISION = "messages-collision";
    public static final String MESSAGES_DISCARDED = "messages-discarded";
    public static final String MESSAGES_PENDING = "messages-pending";
    public static final String MESSAGES_PROCESSOR_CREATED = "messages-processor-created";
    public static final String MESSAGES_PROCESSOR_DELETED = "messages-processor-deleted";
    public static final String OUTPUT_INVOKE_STARTED_TIMER = "output-invoke-started-timer";
    public static final String EVICTION_PASS_STARTED_TIMER = "eviction-pass-started-timer";
    public static final String PRE_INSTANTIATION_STARTED_TIMER = "pre-instantiation-started-timer";

    public static final String[] METRIC_NAMES = new String[] {
        MESSAGES_DISPATCHED,
        MESSAGES_PROCESSED,
        MESSAGES_FAILED,
        MESSAGES_COLLISION,
        MESSAGES_DISCARDED,
        MESSAGES_PROCESSOR_CREATED,
        MESSAGES_PROCESSOR_DELETED,
        OUTPUT_INVOKE_STARTED_TIMER,
        EVICTION_PASS_STARTED_TIMER,
        PRE_INSTANTIATION_STARTED_TIMER,
        MESSAGES_PENDING
    };

    private static class DropwizardTimerContext implements StatsCollector.TimerContext {
        private final Timer timer;
        private final Timer.Context context;

        private DropwizardTimerContext(final MetricRegistry registry, final String timerName) {
            timer = registry.timer(timerName);
            context = timer.time();
        }

        @Override
        public void stop() {
            context.stop();
        }
    }

    private final ClusterId clusterId;

    private final MetricRegistry registry;
    private final Meter messageDispatched;
    private final Meter messageProcessed;
    private final Meter messageFailed;
    private final Meter messageCollision;
    private final Meter messageDiscarded;
    private final Meter messageProcessorCreated;
    private final Meter messageProcessorDeleted;

    private final AtomicLong inProcessMessages = new AtomicLong();

    public DropwizardClusterStatsCollector(final ClusterId clusterId) {
        super();

        this.clusterId = clusterId;

        registry = MetricUtils.getMetricsRegistry();
        messageDispatched = registry.meter(getName(MESSAGES_DISPATCHED));
        messageProcessed = registry.meter(getName(MESSAGES_PROCESSED));
        messageFailed = registry.meter(getName(MESSAGES_FAILED));
        messageCollision = registry.meter(getName(MESSAGES_COLLISION));
        messageDiscarded = registry.meter(getName(MESSAGES_DISCARDED));
        messageProcessorCreated = registry.meter(getName(MESSAGES_PROCESSOR_CREATED));
        messageProcessorDeleted = registry.meter(getName(MESSAGES_PROCESSOR_DELETED));
        registry.gauge(getName(MESSAGES_PENDING), () -> (Gauge<Long>)() -> inProcessMessages.get());
    }

    @Override
    public void messageDispatched(final int num) {
        messageDispatched.mark(num);
        inProcessMessages.getAndAdd(num);
    }

    @Override
    public void messageProcessed(final int num) {
        messageProcessed.mark(num);
        inProcessMessages.getAndAdd(-num);
    }

    @Override
    public void messageFailed(final int num) {
        messageFailed.mark(num);
        inProcessMessages.getAndAdd(-num);
    }

    @Override
    public void messageCollision(final Object message) {
        messageCollision.mark();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messageDiscarded.mark();
    }

    @Override
    public void messageProcessorCreated(final Object key) {
        messageProcessorCreated.mark();
    }

    @Override
    public void messageProcessorDeleted(final Object key) {
        messageProcessorDeleted.mark();
    }

    @Override
    public void stop() {
        // Remove the metrics from the registry
        for(final String m: METRIC_NAMES) {
            registry.remove(getName(m));
        }
    }

    @Override
    public TimerContext preInstantiationStarted() {
        return new DropwizardTimerContext(registry, getName(PRE_INSTANTIATION_STARTED_TIMER));
    }

    @Override
    public TimerContext outputInvokeStarted() {
        return new DropwizardTimerContext(registry, getName(OUTPUT_INVOKE_STARTED_TIMER));
    }

    @Override
    public TimerContext evictionPassStarted() {
        return new DropwizardTimerContext(registry, getName(EVICTION_PASS_STARTED_TIMER));
    }

    // protected access for testing purposes
    protected String getName(final String key) {
        return MetricRegistry.name(DropwizardClusterStatsCollector.class, "cluster", clusterId.applicationName, clusterId.clusterName, key);
    }
}
