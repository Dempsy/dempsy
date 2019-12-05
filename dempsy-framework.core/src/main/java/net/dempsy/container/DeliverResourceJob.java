package net.dempsy.container;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    public void rejected() {
        disposition.dispose(message.message);
        statsCollector.messageDiscarded(message);
        handleDiscardAllContainer();
    }

    private class CJ implements ContainerJob {
        public CJ() {
            disposition.replicate(message.message);
        }

        @Override
        public void execute(final ContainerJobMetadata jobData) {
            try {
                final KeyedMessage km = new KeyedMessage(message.key, message.message);
                jobData.container.dispatch(km, jobData.containerSpecificData, justArrived);
            } finally {
                disposition.dispose(message.message);
            }
        }

        @Override
        public void reject(final ContainerJobMetadata jobData) {
            disposition.dispose(message.message);
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
    public void individuatedJobsComplete() {
        disposition.dispose(message.message);
    }

}
