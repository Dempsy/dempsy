package net.dempsy.container;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.dempsy.container.Container.ContainerSpecific;
import net.dempsy.container.Container.Operation;
import net.dempsy.messages.KeyedMessage;
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
    public void executeAllContainers() {
        executeMessageOnContainers(message, justArrived);
        disposition.dispose(message.message);
    }

    @Override
    public void rejected(final boolean stopping) {
        disposition.dispose(message.message);
        statsCollector.messageDiscarded(message);
    }

    private class CJ extends ContainerJob {
        public CJ(final ContainerSpecific cs) {
            super(cs);
            disposition.replicate(message.message); // unrefed in either execute or reject.
        }

        @Override
        public void execute(final Container container) {
            try {
                dispatch(container, new KeyedMessage(message.key, message.message), Operation.handle, justArrived);
            } finally {
                disposition.dispose(message.message);
            }
        }

        @Override
        public void reject(final Container container) {
            disposition.dispose(message.message);
            reject(container, new KeyedMessage(message.key, message.message), justArrived);
        }

    }

    @Override
    public List<ContainerJob> individuate() {
        // disposition.replicate(message.message); // unrefed in individuatedJobsComplete
        return Arrays.stream(containerData())
            .map(c -> c.messageBeingEnqueudExternally(new KeyedMessage(message.key, message.message), justArrived))
            .map(i -> new CJ(i))
            .collect(Collectors.toList());
    }

    @Override
    public void individuatedJobsComplete() {
        disposition.dispose(message.message);
    }
}
