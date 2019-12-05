package net.dempsy.container;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.RoutedMessage;

public class DefaultDeliverMessageJob extends DeliverMessageJob {

    public DefaultDeliverMessageJob(final Container[] containers, final NodeStatsCollector statsCollector, final RoutedMessage message,
        final boolean justArrived) {
        super(containers, statsCollector, message, justArrived);
    }

    @Override
    public void executeAllContainers() {
        executeMessageOnContainers(message, justArrived);
    }

    @Override
    public void rejected() {
        statsCollector.messageDiscarded(message);
        handleDiscardAllContainer();
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
}
