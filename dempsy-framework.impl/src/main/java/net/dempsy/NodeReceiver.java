package net.dempsy;

import java.util.List;
import java.util.function.Supplier;

import net.dempsy.container.Container;
import net.dempsy.container.DefaultDeliverMessageJob;
import net.dempsy.container.DeliverDelayedMessageJob;
import net.dempsy.container.DeliverResourceJob;
import net.dempsy.container.MessageDeliveryJob;
import net.dempsy.messages.MessageResourceManager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.RoutedMessage;

public class NodeReceiver implements Listener<RoutedMessage> {

    private final Container[] containers;
    private final ThreadingModel threadModel;
    private final NodeStatsCollector statsCollector;

    public NodeReceiver(final List<Container> nodeContainers, final ThreadingModel threadModel, final NodeStatsCollector statsCollector) {
        containers = nodeContainers.toArray(new Container[nodeContainers.size()]);
        this.threadModel = threadModel;
        this.statsCollector = statsCollector;
    }

    private static final boolean ON_MESSAGE_JUST_ARRIVED = true;

    @Override
    public boolean onMessage(final RoutedMessage message) throws MessageTransportException {
        statsCollector.messageReceived(message);
        propogateMessageToNode(message, ON_MESSAGE_JUST_ARRIVED, null);
        return true;
    }

    @Override
    public boolean onMessage(final Supplier<RoutedMessage> supplier) {
        statsCollector.messageReceived(supplier);
        threadModel.submitLimited(new DeliverDelayedMessageJob(containers, statsCollector, supplier, ON_MESSAGE_JUST_ARRIVED));
        return true;
    }

    /**
     * This passes the message directly to the current node container(s) listed in the message.
     *
     * If the message is a resource (and therefore disposition isn't null) there's an assumption that
     * the message is "opened" and responsibility for the closing of it is being passed along to propogateMessageToNode
     *
     */
    public void propogateMessageToNode(final RoutedMessage message, final boolean justArrived, final MessageResourceManager disposition) {
        if(disposition == null) {
            final MessageDeliveryJob rejectable = new DefaultDeliverMessageJob(containers, statsCollector, message, justArrived);
            if(justArrived)
                threadModel.submitLimited(rejectable);
            else
                threadModel.submit(rejectable);
        } else {
            final MessageDeliveryJob rejectable = new DeliverResourceJob(containers, statsCollector, message, justArrived, disposition);
            if(justArrived)
                threadModel.submitLimited(rejectable);
            else {
                if(message.message.getClass().getSimpleName().equals("ThermalSamplingIntervalByCamera")) {
                    int i = 0;
                    i += 13;
                    dump(i);
                }
                threadModel.submit(rejectable);
            }
        }
    }

    private static void dump(final int i) {

    }
}
