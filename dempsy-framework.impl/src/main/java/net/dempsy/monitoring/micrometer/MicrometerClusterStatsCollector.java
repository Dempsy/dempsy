package net.dempsy.monitoring.micrometer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.StatsCollector;

public class MicrometerClusterStatsCollector implements ClusterStatsCollector {

    public static final String MESSAGES_DISPATCHED = "dempsy.cluster.messages.dispatched";
    public static final String MESSAGES_PROCESSED = "dempsy.cluster.messages.processed";
    public static final String MESSAGES_FAILED = "dempsy.cluster.messages.failed";
    public static final String MESSAGES_COLLISION = "dempsy.cluster.messages.collision";
    public static final String MESSAGES_DISCARDED = "dempsy.cluster.messages.discarded";
    public static final String MESSAGES_PENDING = "dempsy.cluster.messages.pending";
    public static final String MESSAGES_PROCESSOR_CREATED = "dempsy.cluster.processor.created";
    public static final String MESSAGES_PROCESSOR_DELETED = "dempsy.cluster.processor.deleted";
    public static final String OUTPUT_INVOKE_TIMER = "dempsy.cluster.output.invoke";
    public static final String EVICTION_PASS_TIMER = "dempsy.cluster.eviction.pass";
    public static final String PRE_INSTANTIATION_TIMER = "dempsy.cluster.pre.instantiation";

    private static class MicrometerTimerContext implements StatsCollector.TimerContext {
        private final Timer.Sample sample;
        private final Timer timer;

        private MicrometerTimerContext(final MeterRegistry registry, final String timerName, final String app, final String cluster) {
            this.sample = Timer.start(registry);
            this.timer = Timer.builder(timerName)
                .tag("app", app)
                .tag("cluster", cluster)
                .register(registry);
        }

        @Override
        public void stop() {
            sample.stop(timer);
        }
    }

    private final ClusterId clusterId;
    private final MeterRegistry registry;
    private final Counter messageDispatched;
    private final Counter messageProcessed;
    private final Counter messageFailed;
    private final Counter messageCollision;
    private final Counter messageDiscarded;
    private final Counter messageProcessorCreated;
    private final Counter messageProcessorDeleted;
    private final AtomicLong inProcessMessages = new AtomicLong();
    private final List<io.micrometer.core.instrument.Meter.Id> registeredMeterIds = new ArrayList<>();

    public MicrometerClusterStatsCollector(final ClusterId clusterId, final MeterRegistry registry) {
        this.clusterId = clusterId;
        this.registry = registry;

        final String app = clusterId.applicationName;
        final String cluster = clusterId.clusterName;

        messageDispatched = Counter.builder(MESSAGES_DISPATCHED).tag("app", app).tag("cluster", cluster).register(registry);
        messageProcessed = Counter.builder(MESSAGES_PROCESSED).tag("app", app).tag("cluster", cluster).register(registry);
        messageFailed = Counter.builder(MESSAGES_FAILED).tag("app", app).tag("cluster", cluster).register(registry);
        messageCollision = Counter.builder(MESSAGES_COLLISION).tag("app", app).tag("cluster", cluster).register(registry);
        messageDiscarded = Counter.builder(MESSAGES_DISCARDED).tag("app", app).tag("cluster", cluster).register(registry);
        messageProcessorCreated = Counter.builder(MESSAGES_PROCESSOR_CREATED).tag("app", app).tag("cluster", cluster).register(registry);
        messageProcessorDeleted = Counter.builder(MESSAGES_PROCESSOR_DELETED).tag("app", app).tag("cluster", cluster).register(registry);
        Gauge.builder(MESSAGES_PENDING, inProcessMessages, AtomicLong::get).tag("app", app).tag("cluster", cluster).register(registry);

        registeredMeterIds.add(messageDispatched.getId());
        registeredMeterIds.add(messageProcessed.getId());
        registeredMeterIds.add(messageFailed.getId());
        registeredMeterIds.add(messageCollision.getId());
        registeredMeterIds.add(messageDiscarded.getId());
        registeredMeterIds.add(messageProcessorCreated.getId());
        registeredMeterIds.add(messageProcessorDeleted.getId());
    }

    @Override
    public void messageDispatched(final int num) {
        messageDispatched.increment(num);
        inProcessMessages.getAndAdd(num);
    }

    @Override
    public void messageProcessed(final int num) {
        messageProcessed.increment(num);
        inProcessMessages.getAndAdd(-num);
    }

    @Override
    public void messageFailed(final int num) {
        messageFailed.increment(num);
        inProcessMessages.getAndAdd(-num);
    }

    @Override
    public void messageCollision(final Object message) {
        messageCollision.increment();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messageDiscarded.increment();
    }

    @Override
    public void messageProcessorCreated(final Object key) {
        messageProcessorCreated.increment();
    }

    @Override
    public void messageProcessorDeleted(final Object key) {
        messageProcessorDeleted.increment();
    }

    @Override
    public void stop() {
        registry.getMeters().stream()
            .filter(m -> {
                final String app = m.getId().getTag("app");
                final String cluster = m.getId().getTag("cluster");
                return clusterId.applicationName.equals(app) && clusterId.clusterName.equals(cluster);
            })
            .forEach(registry::remove);
    }

    @Override
    public TimerContext preInstantiationStarted() {
        return new MicrometerTimerContext(registry, PRE_INSTANTIATION_TIMER, clusterId.applicationName, clusterId.clusterName);
    }

    @Override
    public TimerContext outputInvokeStarted() {
        return new MicrometerTimerContext(registry, OUTPUT_INVOKE_TIMER, clusterId.applicationName, clusterId.clusterName);
    }

    @Override
    public TimerContext evictionPassStarted() {
        return new MicrometerTimerContext(registry, EVICTION_PASS_TIMER, clusterId.applicationName, clusterId.clusterName);
    }

    MeterRegistry getRegistry() {
        return registry;
    }

    ClusterId getClusterId() {
        return clusterId;
    }
}
