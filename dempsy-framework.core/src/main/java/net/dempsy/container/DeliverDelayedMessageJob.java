package net.dempsy.container;

import java.util.Arrays;
import java.util.function.Supplier;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DeliverDelayedMessageJob implements ContainerJob {
    private final Supplier<RoutedMessage> messageSupplier;
    protected final boolean justArrived;
    protected final NodeStatsCollector statsCollector;
    final Container[] containers;

    private RoutedMessage message = null;
    private ContainerJobMetadata[] deliveries = null;
    private boolean containersCalculated = false;

    public DeliverDelayedMessageJob(final Container[] containers, final NodeStatsCollector statsCollector, final Supplier<RoutedMessage> messageSupplier,
        final boolean justArrived) {
        this.messageSupplier = messageSupplier;
        this.justArrived = justArrived;
        this.statsCollector = statsCollector;
        this.containers = containers;
    }

    @Override
    public Object call() throws Exception {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);
        Arrays.stream(message.containers)
            .forEach(i -> containers[i].dispatch(km, null, justArrived));
        return null;
    }

    @Override
    public void rejected() {
        statsCollector.messageDiscarded(messageSupplier);
    }

    @Override
    public synchronized boolean containersCalculated() {
        return containersCalculated;
    }

    @Override
    public ContainerJobMetadata[] containerData() {
        return deliveries;
    }

    @Override
    public void calculateContainers() {
        try {
            message = messageSupplier.get();
            this.deliveries = Arrays.stream(message.containers)
                .mapToObj(ci -> containers[ci])
                .map(c -> new ContainerJobMetadata(c, c.prepareMessage(message, justArrived)))
                .toArray(ContainerJobMetadata[]::new);
        } finally { // no matter what, if calculateContainers was called, then this must be set.
            synchronized(this) {
                containersCalculated = true;
            }
        }
    }
}
