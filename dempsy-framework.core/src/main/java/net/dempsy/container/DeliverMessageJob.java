package net.dempsy.container;

import java.util.Arrays;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public abstract class DeliverMessageJob implements ContainerJob {
    protected final boolean justArrived;
    protected final NodeStatsCollector statsCollector;
    protected final RoutedMessage message;

    protected final ContainerJobMetadata[] deliveries;

    protected DeliverMessageJob(final Container[] allContainers, final NodeStatsCollector statsCollector, final RoutedMessage message,
        final boolean justArrived) {
        this.message = message;
        this.justArrived = justArrived;
        this.statsCollector = statsCollector;
        this.deliveries = Arrays.stream(message.containers)
            .mapToObj(ci -> allContainers[ci])
            .map(c -> new ContainerJobMetadata(c, c.prepareMessage(message, justArrived)))
            .toArray(ContainerJobMetadata[]::new);
    }

    @Override
    public boolean containersCalculated() {
        return true;
    }

    @Override
    public void calculateContainers() {}

    @Override
    public ContainerJobMetadata[] containerData() {
        return deliveries;
    }

    protected void executeMessageOnContainers(final RoutedMessage message, final boolean justArrived) {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);

        Arrays.stream(deliveries)
            .forEach(d -> d.c.dispatch(km, d.p, justArrived));
    }

    protected void handleDiscardContainer() {
        Arrays.stream(deliveries)
            .map(d -> d.p)
            .filter(p -> p != null)
            .forEach(p -> p.messageBeingDiscarded());
    }
}
