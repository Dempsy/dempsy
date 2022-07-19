package net.dempsy.container;

import java.util.Arrays;

import net.dempsy.container.Container.Operation;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public abstract class DeliverMessageJob implements MessageDeliveryJob {
    protected final boolean justArrived;
    protected final NodeStatsCollector statsCollector;
    protected final RoutedMessage message;

    private Container[] deliveries;
    private boolean executeCalled = false;
    protected Container[] allContainers;

    protected DeliverMessageJob(final Container[] allContainers, final NodeStatsCollector statsCollector, final RoutedMessage message,
        final boolean justArrived) {
        this.message = message;
        this.justArrived = justArrived;
        this.statsCollector = statsCollector;
        this.deliveries = Arrays.stream(message.containers)
            .mapToObj(ci -> allContainers[ci])
            .toArray(Container[]::new);
    }

    @Override
    public boolean containersCalculated() {
        return true;
    }

    @Override
    public void calculateContainers() {}

    @Override
    public Container[] containerData() {
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
            .forEach(c -> c.dispatch(km, Operation.handle, justArrived));
    }
}
