package net.dempsy;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import net.dempsy.container.Container;
import net.dempsy.container.Container.ContainerSpecific;
import net.dempsy.messages.KeyedMessage;
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
        threadModel.submitLimited(new DeliverDelayedMessage(containers, statsCollector, supplier, ON_MESSAGE_JUST_ARRIVED));
        return true;
    }

    /**
     * This passes the message directly to the current node container(s) listed in the message.
     *
     * If the message is a resource (and therefore disposition isn't null) there's an assumption that
     * the message is "opened" and responsibility for the closing of it is being passed along to feedbackLoop
     *
     */
    public void propogateMessageToNode(final RoutedMessage message, final boolean justArrived, final MessageResourceManager disposition) {
        if(disposition == null) {
            final ThreadingModel.Rejectable<?> rejectable = new DeliverMessage(containers, statsCollector, message, justArrived);
            if(justArrived)
                threadModel.submitLimited(rejectable);
            else
                threadModel.submit(rejectable);
        } else {
            final ThreadingModel.Rejectable<?> rejectable = new DeliverResource(containers, statsCollector, message, justArrived, disposition);
            if(justArrived)
                threadModel.submitLimited(rejectable);
            else
                threadModel.submit(rejectable);
        }
    }

    private static final class Delivery {
        public final Container c;
        public final ContainerSpecific p;

        public Delivery(final Container c, final ContainerSpecific p) {
            this.c = c;
            this.p = p;
        }
    }

    private static abstract class Deliver implements ThreadingModel.Rejectable<Object> {
        protected final boolean justArrived;
        protected final NodeStatsCollector statsCollector;
        protected final RoutedMessage message;

        protected final Delivery[] deliveries;

        protected Deliver(final Container[] allContainers, final NodeStatsCollector statsCollector, final RoutedMessage message, final boolean justArrived) {
            this.message = message;
            this.justArrived = justArrived;
            this.statsCollector = statsCollector;
            this.deliveries = Arrays.stream(message.containers)
                .mapToObj(ci -> allContainers[ci])
                .map(c -> new Delivery(c, c.prepareMessage(message, justArrived)))
                .toArray(Delivery[]::new);
        }

        protected void executeMessageOnContainers(final RoutedMessage message, final boolean justArrived) {
            final KeyedMessage km = new KeyedMessage(message.key, message.message);

            Arrays.stream(deliveries)
                .forEach(d -> d.c.dispatch(km, d.p, justArrived));
        }

        protected void handleDiscardContainer() {
            Arrays.stream(deliveries)
                .map(d -> d.p)
                .filter(p -> p != null)
                .forEach(p -> p.messageBeingDiscarded());
        }

    }

    private static class DeliverResource extends Deliver {
        private final MessageResourceManager disposition;

        public DeliverResource(final Container[] containers, final NodeStatsCollector statsCollector, final RoutedMessage message, final boolean justArrived,
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

    private class DeliverMessage extends Deliver {

        public DeliverMessage(final Container[] containers, final NodeStatsCollector statsCollector, final RoutedMessage message, final boolean justArrived) {
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

    private class DeliverDelayedMessage implements ThreadingModel.Rejectable<Object> {
        private final Supplier<RoutedMessage> messageSupplier;
        protected final boolean justArrived;
        protected final NodeStatsCollector statsCollector;
        final Container[] containers;

        public DeliverDelayedMessage(final Container[] containers, final NodeStatsCollector statsCollector, final Supplier<RoutedMessage> messageSupplier,
            final boolean justArrived) {
            this.messageSupplier = messageSupplier;
            this.justArrived = justArrived;
            this.statsCollector = statsCollector;
            this.containers = containers;
        }

        @Override
        public Object call() throws Exception {
            // we need to process the message. Currently (11/20/2019) the only way to get a
            // Supplier<RoutedMessage> is if we 'justArrived' since it's used for parallelizing
            // deserialization of multiple messages (without this the receiver thread for the
            // transport would be required to do that work). Therefore, we're going to skip the
            // container prepareMessage call. We couldn't do it before because we don't know
            // what container(s) this message will go to without being able to see the 'containers'
            // field on the constituted message but we don't want to get the Supplier<>.get
            // call from the other thread. So we're *OL without a refactor.
            final RoutedMessage message = messageSupplier.get();
            final KeyedMessage km = new KeyedMessage(message.key, message.message);
            Arrays.stream(message.containers)
                .forEach(i -> containers[i].dispatch(km, null, justArrived));
            return null;
        }

        @Override
        public void rejected() {
            statsCollector.messageDiscarded(messageSupplier);
        }
    }
}
