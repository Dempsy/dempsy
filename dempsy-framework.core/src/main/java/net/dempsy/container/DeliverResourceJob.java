package net.dempsy.container;

import net.dempsy.messages.MessageResourceManager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DeliverResourceJob extends DeliverMessageJob {
    private final MessageResourceManager disposition;

    public DeliverResourceJob(final Container[] containers, final NodeStatsCollector statsCollector, final RoutedMessage message, final boolean justArrived,
        final MessageResourceManager disposition) {
        super(containers, statsCollector, message, justArrived);
        this.disposition = disposition;
    }

    @Override
    public Object call() throws Exception {
        executeMessageOnContainers(message, justArrived);
        disposition.dispose(message.message);
        return null;
    }

    @Override
    public void rejected() {
        disposition.dispose(message.message);
        statsCollector.messageDiscarded(message);
        handleDiscardContainer();
    }
}