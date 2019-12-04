package net.dempsy.container;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DefaultDeliverMessageJob extends DeliverMessageJob {

    public DefaultDeliverMessageJob(final Container[] containers, final NodeStatsCollector statsCollector, final RoutedMessage message,
        final boolean justArrived) {
        super(containers, statsCollector, message, justArrived);
    }

    @Override
    public Object call() throws Exception {
        executeMessageOnContainers(message, justArrived);
        return null;
    }

    @Override
    public void rejected() {
        statsCollector.messageDiscarded(message);
        handleDiscardContainer();
    }
}