package net.dempsy.container;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.dempsy.container.Container.ContainerSpecific;
import net.dempsy.container.Container.Operation;
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
    public void rejected(final boolean stopping) {
   		statsCollector.messageDiscarded(message);
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
    	return Arrays.stream(containerData())
    			.map(c -> c.messageBeingEnqueudExternally(new KeyedMessage(message.key, message.message), justArrived))
    			.map(i -> new CJ(i))
    			.collect(Collectors.toList());
    }
}
