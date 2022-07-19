package net.dempsy.container;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.dempsy.container.Container.ContainerSpecific;
import net.dempsy.container.Container.Operation;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DeliverDelayedMessageJob implements MessageDeliveryJob {
    private final Supplier<RoutedMessage> messageSupplier;
    protected final boolean justArrived;
    protected final NodeStatsCollector statsCollector;
    final Container[] allContainers;

    private RoutedMessage message = null;
    private Container[] deliveries = null;
    private boolean containersCalculated = false;

    public DeliverDelayedMessageJob(final Container[] containers, final NodeStatsCollector statsCollector, final Supplier<RoutedMessage> messageSupplier,
        final boolean justArrived) {
        this.messageSupplier = messageSupplier;
        this.justArrived = justArrived;
        this.statsCollector = statsCollector;
        this.allContainers = containers;
    }

    @Override
    public void executeAllContainers() {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);
        Arrays.stream(message.containers)
            .forEach(i ->

            allContainers[i].dispatch(km, Operation.handle, justArrived));
    }

    @Override
    public void rejected(final boolean stopping) {
        statsCollector.messageDiscarded(messageSupplier);
    }

    @Override
    public synchronized boolean containersCalculated() {
        return containersCalculated;
    }

    @Override
    public Container[] containerData() {
        return deliveries;
    }

    @Override
    public void calculateContainers() {
        try {
            message = messageSupplier.get();
            this.deliveries = Arrays.stream(message.containers)
                .mapToObj(ci -> allContainers[ci])
                .toArray(Container[]::new);
        } finally { // no matter what, if calculateContainers was called, then this must be set.
            synchronized(this) {
                containersCalculated = true;
            }
        }
    }

    private class CJ extends ContainerJob {
    	CJ(ContainerSpecific cs) {
    		super(cs);
    	}
    	
        @Override
        public void execute(final Container container) {
            dispatch(container, new KeyedMessage(message.key, message.message), Operation.handle, justArrived);
        }

        @Override
        public void reject(final Container container) {
        	reject(container, new KeyedMessage(message.key, message.message), justArrived);
        }
    }

    @Override
    public List<ContainerJob> individuate() {
    	return Arrays.stream(deliveries)
    			.map(c -> c.messageBeingEnqueudExternally(new KeyedMessage(message.key, message.message), justArrived))
    			.map(i -> new CJ(i))
    			.collect(Collectors.toList());
    }

    @Override
    public void individuatedJobsComplete() {}
}
