package net.dempsy.container;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DeliverDelayedMessageJob implements MessageDeliveryJob {
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
    public void executeAllContainers() {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);
        Arrays.stream(message.containers)
            .forEach(i -> containers[i].dispatch(km, null, justArrived));
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

    private class CJ implements ContainerJob {
        @Override
        public void execute(final ContainerJobMetadata jobData) {
            final KeyedMessage km = new KeyedMessage(message.key, message.message);

            jobData.container.dispatch(km, jobData.containerSpecificData, justArrived);
        }

        @Override
        public void reject(final ContainerJobMetadata jobData) {
            if(jobData.containerSpecificData != null)
                jobData.containerSpecificData.messageBeingDiscarded();
        }

    }

    @Override
    public List<ContainerJob> individuate() {
        return IntStream.range(0, deliveries.length)
            .mapToObj(i -> new CJ())
            .collect(Collectors.toList());
    }

    @Override
    public void individuatedJobsComplete() {}
}
