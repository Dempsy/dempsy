package net.dempsy.monitoring.dropwizard;

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
    public static final String MESSAGES_PROCESSOR_CREATED = "messages-processor-created";
    public static final String MESSAGES_PROCESSOR_DELETED = "messages-processor-deleted";
    public static final String OUTPUT_INVOKE_STARTED_TIMER = "output-invoke-started-timer";
    public static final String EVICTION_PASS_STARTED_TIMER = "eviction-pass-started-timer";
    public static final String PRE_INSTANTIATION_STARTED_TIMER = "pre-instantiation-started-timer";

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
    private final Meter messageProcessorCreated;
    private final Meter messageProcessorDeleted;

    public DropwizardClusterStatsCollector(final ClusterId clusterId) {
        super();

        this.clusterId = clusterId;

        registry = MetricUtils.getMetricsRegistry();
        messageDispatched = registry.meter(getName(MESSAGES_DISPATCHED));
        messageProcessed = registry.meter(getName(MESSAGES_PROCESSED));
        messageFailed = registry.meter(getName(MESSAGES_FAILED));
        messageCollision = registry.meter(getName(MESSAGES_COLLISION));
        messageProcessorCreated = registry.meter(getName(MESSAGES_PROCESSOR_CREATED));
        messageProcessorDeleted = registry.meter(getName(MESSAGES_PROCESSOR_DELETED));
    }

    @Override
    public void messageDispatched(final Object message) {
        messageDispatched.mark();
    }

    @Override
    public void messageProcessed(final Object message) {
        messageProcessed.mark();
    }

    @Override
    public void messageFailed(final boolean mpFailure) {
        messageFailed.mark();
    }

    @Override
    public void messageCollision(final Object message) {
        messageCollision.mark();
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
        // Don't need to do anything to stop this collector.
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
