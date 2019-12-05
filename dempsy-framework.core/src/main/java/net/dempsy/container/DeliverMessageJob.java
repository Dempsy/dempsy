package net.dempsy.container;

import java.util.Arrays;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public abstract class DeliverMessageJob implements MessageDeliveryJob {
    protected final boolean justArrived;
    protected final NodeStatsCollector statsCollector;
    protected final RoutedMessage message;

    protected final ContainerJobMetadata[] deliveries;
    protected boolean executeCalled = false;

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

    /**
     * This does nothing by default.
     */
    @Override
    public void individuatedJobsComplete() {}

    protected void executeMessageOnContainers(final RoutedMessage message, final boolean justArrived) {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);

        Arrays.stream(deliveries)
            .forEach(d -> d.container.dispatch(km, d.containerSpecificData, justArrived));
    }

    protected void handleDiscardAllContainer() {
        Arrays.stream(deliveries)
            .map(d -> d.containerSpecificData)
            .filter(p -> p != null)
            .forEach(p -> p.messageBeingDiscarded());
    }

    protected ContainerJobMetadata lookupContainerSpecific(final Container c) {
        return Arrays.stream(deliveries)
            .filter(d -> d.container == c)
            .findAny()
            .orElse(null);
    }
}
