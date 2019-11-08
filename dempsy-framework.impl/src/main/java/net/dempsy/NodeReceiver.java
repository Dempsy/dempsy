package net.dempsy;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.Container;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.MessageResourceManager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.util.SafeString;

public class NodeReceiver implements Listener<RoutedMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeReceiver.class);
    private static final boolean traceEnabled = LOGGER.isTraceEnabled();

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
        feedbackLoop(message, true, null);
        return true;
    }

    @Override
    public boolean onMessage(final Supplier<RoutedMessage> supplier) {
        statsCollector.messageReceived(supplier);
        threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

            @Override
            public Object call() throws Exception {
                final RoutedMessage message = supplier.get();
                if(traceEnabled)
                    LOGGER.trace("Received message {} with key {}", SafeString.valueOf(message.message), SafeString.valueOf(message.key));
                doIt(message, ON_MESSAGE_JUST_ARRIVED);
                return null;
            }

            @Override
            public void rejected() {
                statsCollector.messageDiscarded(supplier);
            }
        }, ON_MESSAGE_JUST_ARRIVED);
        return true;
    }

    private void doIt(final RoutedMessage message, final boolean justArrived) {
        final KeyedMessage km = new KeyedMessage(message.key, message.message);
        Arrays.stream(message.containers)
            .forEach(container -> containers[container]
                .dispatch(km, true, justArrived));
    }

    /**
     * This passes the message directly to the current node container(s) listed in the message.
     *
     * If the message is a resource (and therefore disposition isn't null) there's an assumption that
     * the message is "opened" and responsibility for the closing of it is being passed along to feedbackLoop
     *
     */
    public void feedbackLoop(final RoutedMessage message, final boolean justArrived, final MessageResourceManager disposition) {
        if(disposition == null) {
            threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

                @Override
                public Object call() throws Exception {
                    doIt(message, justArrived);
                    return null;
                }

                @Override
                public void rejected() {
                    statsCollector.messageDiscarded(message);
                }
            }, justArrived);
        } else {

            threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

                @Override
                public Object call() throws Exception {
                    doIt(message, justArrived);
                    if(disposition != null)
                        disposition.dispose(message.message);
                    return null;
                }

                @Override
                public void rejected() {
                    if(disposition != null)
                        disposition.dispose(message.message);
                    statsCollector.messageDiscarded(message);
                }
            }, justArrived);
        }
    }
}
