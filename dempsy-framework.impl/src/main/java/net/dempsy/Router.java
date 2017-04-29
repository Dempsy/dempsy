package net.dempsy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.TransportManager;
import net.dempsy.util.SafeString;
import net.dempsy.utils.PersistentTask;

public class Router extends Dispatcher implements Service {
    private static Logger LOGGER = LoggerFactory.getLogger(Router.class);
    private static final long RETRY_TIMEOUT = 500L;

    private PersistentTask checkup;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final RoutingStrategyManager manager;
    private final AtomicReference<ApplicationState> outbounds = new AtomicReference<>(null);

    private final NodeAddress thisNode;
    private final TransportManager tmanager;
    private final NodeReceiver nodeReciever;
    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final NodeStatsCollector statsCollector;

    private static class ApplicationState {
        private final Map<String, RoutingStrategy.Router> outboundByClusterName;
        private final Map<String, List<String>> clusterNameByMessageType;
        private final Map<NodeAddress, Sender> senderByNode;
        private final Map<NodeAddress, NodeInformation> current;

        ApplicationState(final Map<String, net.dempsy.router.RoutingStrategy.Router> outboundByClusterName,
                final Map<String, Set<String>> cnByType, final Map<NodeAddress, Sender> senderByNode,
                final Map<NodeAddress, NodeInformation> current) {
            this.outboundByClusterName = outboundByClusterName;
            this.clusterNameByMessageType = new HashMap<>();
            cnByType.entrySet().forEach(e -> clusterNameByMessageType.put(e.getKey(), new ArrayList<String>(e.getValue())));
            this.senderByNode = senderByNode;
            this.current = current;
        }

        ApplicationState() {
            outboundByClusterName = new HashMap<>();
            clusterNameByMessageType = new HashMap<>();
            senderByNode = new HashMap<>();
            current = new HashMap<>();
        }

        public static class Update {
            final Set<NodeInformation> toAdd;
            final Set<NodeAddress> toDelete;
            final Set<NodeInformation> leaveAlone;
            final NodeAddress thisNode;

            public Update(final Set<NodeInformation> leaveAlone, final Set<NodeInformation> toAdd, final Set<NodeAddress> toDelete,
                    final NodeAddress thisNode) {
                this.toAdd = toAdd;
                this.toDelete = toDelete;
                this.leaveAlone = leaveAlone;
                this.thisNode = thisNode;
            }

            public boolean change() {
                return (!(toDelete.size() == 0 && toAdd.size() == 0));
            }
        }

        public Update update(final Set<NodeInformation> newState, final NodeAddress thisNode) {
            final Set<NodeInformation> toAdd = new HashSet<>();
            final Set<NodeAddress> toDelete = new HashSet<>();
            final Set<NodeAddress> knownAddr = new HashSet<>(current.keySet());
            final Set<NodeInformation> leaveAlone = new HashSet<>();

            NodeAddress oursOnTheList = null;

            for (final NodeInformation cur : newState) {
                final NodeInformation known = current.get(cur.nodeAddress);
                if (cur.nodeAddress.equals(thisNode))
                    oursOnTheList = cur.nodeAddress;
                if (known == null) // then we don't know about this one yet.
                    // we need to add this one
                    toAdd.add(cur);
                else {
                    if (!known.equals(cur)) { // known but changed ... we need to add and delete it
                        toAdd.add(cur);
                        toDelete.add(known.nodeAddress);
                    } else
                        leaveAlone.add(known);

                    // remove it from the known ones. Whatever is leftover will
                    // end up needing to be deleted.
                    knownAddr.remove(known.nodeAddress);
                }
            }

            if (oursOnTheList == null && thisNode != null) // we don't seem to have our own address registered with the collaborator.
                                                           // this condition is actually okay since the Router is started before the
                                                           // node is registered with the collaborator.
                LOGGER.trace("Router at {} doesn't seem to have its own address registered with the collaborator yet", thisNode);

            // dump the remaining knownAddrs on the toDelete list
            toDelete.addAll(knownAddr);

            return new Update(leaveAlone, toAdd, toDelete, thisNode);
        }

        public ApplicationState apply(final Update update, final TransportManager tmanager, final NodeStatsCollector statsCollector,
                final RoutingStrategyManager manager) {
            // apply toDelete first.
            final Set<NodeAddress> toDelete = update.toDelete;

            for (final NodeAddress cur : toDelete) {
                senderByNode.get(current.get(cur).nodeAddress).stop();
            }

            final Map<NodeAddress, NodeInformation> newCurrent = new HashMap<>();
            final Map<NodeAddress, Sender> newSenders = new HashMap<>();

            // the one's to carry over.
            final Set<NodeInformation> leaveAlone = update.leaveAlone;
            for (final NodeInformation cur : leaveAlone) {
                // if this is our node then we don't want to add a sender.
                // update.thisNode can be null but cur.nodeAddress cannot
                if (!cur.nodeAddress.equals(update.thisNode)) {
                    final Sender sender = senderByNode.get(cur.nodeAddress);
                    newSenders.put(cur.nodeAddress, sender);
                }
                newCurrent.put(cur.nodeAddress, cur);
            }

            // add new senders
            final Set<NodeInformation> toAdd = update.toAdd;
            for (final NodeInformation cur : toAdd) {
                if (!cur.nodeAddress.equals(update.thisNode)) {
                    final SenderFactory sf = tmanager.getAssociatedInstance(cur.transportTypeId);
                    final Sender sender = sf.getSender(cur.nodeAddress);
                    newSenders.put(cur.nodeAddress, sender);
                }
                newCurrent.put(cur.nodeAddress, cur);
            }

            // now flush out the remaining caches.

            // collapse all clusterInfos
            final Set<ClusterInformation> allCis = new HashSet<>();
            newCurrent.values().forEach(ni -> allCis.addAll(ni.clusterInfoByClusterId.values()));

            final Map<String, RoutingStrategy.Router> newOutboundByClusterName = new HashMap<>();
            final Map<String, Set<String>> cnByType = new HashMap<>();

            final Set<String> knownClusterOutbounds = new HashSet<>(outboundByClusterName.keySet());

            for (final ClusterInformation ci : allCis) {
                final String clusterName = ci.clusterId.clusterName;
                final RoutingStrategy.Router ob = outboundByClusterName.get(clusterName);
                knownClusterOutbounds.remove(clusterName);
                if (ob != null)
                    newOutboundByClusterName.put(clusterName, ob);
                else {
                    final RoutingStrategy.Factory obfactory = manager.getAssociatedInstance(ci.routingStrategyTypeId);
                    final RoutingStrategy.Router nob = obfactory.getStrategy(ci.clusterId);
                    newOutboundByClusterName.put(clusterName, nob);
                }

                // add all of the message types handled.
                ci.messageTypesHandled.forEach(mt -> {
                    Set<String> entry = cnByType.get(mt);
                    if (entry == null) {
                        entry = new HashSet<>();
                        cnByType.put(mt, entry);
                    }
                    entry.add(clusterName);
                });
            }

            return new ApplicationState(newOutboundByClusterName, cnByType, newSenders, newCurrent);
        }

    }

    public Router(final RoutingStrategyManager manager, final NodeAddress thisNode, final NodeReceiver nodeReciever,
            final TransportManager tmanager, final NodeStatsCollector statsCollector) {
        this.manager = manager;
        this.thisNode = thisNode;
        this.tmanager = tmanager;
        this.nodeReciever = nodeReciever;
        this.statsCollector = statsCollector;
    }

    @Override
    public void dispatch(final KeyedMessageWithType message) {
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("Dispatching " + SafeString.objectDescription(message.message) + ".");

        boolean messageSentSomewhere = false;
        try {

            ApplicationState tmp = outbounds.get();

            // if we're in the midst of an update then we really want to wait for the new state.
            while (tmp == null) {
                if (!isRunning.get())
                    throw new DempsyException("Router dispatch called while stopped.");

                if (!isReady.get()) // however, if we never were ready then we're not in the midst
                                    // of an update.
                    throw new IllegalStateException("Dispatch used before Router is ready.");

                Thread.yield(); // let the other threads do their thing. Maybe we'll be updated sooner.
                tmp = outbounds.get(); // are we updated yet?
            }
            final ApplicationState cur = tmp;

            final Map<String, List<String>> clusterNameByMessageType = cur.clusterNameByMessageType;
            final Map<String, RoutingStrategy.Router> outboundByClusterName = cur.outboundByClusterName;
            final Set<String> uniqueClusters = new HashSet<>();

            // =================================================================================
            // For each message type, determine the set of clusters that this message needs to
            // be sent to. The goal of this loop is to set 'uniqueClusters' to the set of
            // cluster names that 'message' will be sent to
            for (final String mt : message.messageTypes) {
                final List<String> clusterNames = clusterNameByMessageType.get(mt);
                if (clusterNames == null)
                    LOGGER.trace("No cluster that handles messages of type {}", mt);
                else
                    uniqueClusters.addAll(clusterNames);
            }
            // =================================================================================

            // =================================================================================
            // For each cluster that 'message' will be sent to, find the specific containers
            // within the clusters that it will be sent to.
            final Map<NodeAddress, ContainerAddress> containerByNodeAddress = new HashMap<>();
            // build the list of container addresses by node address by accumulating addresses
            for (final String clusterName : uniqueClusters) {
                final RoutingStrategy.Router ob = outboundByClusterName.get(clusterName);
                final ContainerAddress ca = ob.selectDestinationForMessage(message);
                // it's possible 'ca' is null when we don't know where to send the message.
                if (ca == null)
                    LOGGER.info("No way to send the message {} to the cluster {} for the time being", message.message, clusterName);
                else {
                    // When the message will be sent to 2 different clusters, but both clusters
                    // are hosted in the same node, then we send 1 message to 1 ContainerAddress
                    // where the 'clusters' field contains both container ids.
                    final ContainerAddress already = containerByNodeAddress.get(ca.node);
                    if (already != null) {
                        final int[] ia = new int[already.clusters.length + ca.clusters.length];
                        System.arraycopy(already.clusters, 0, ia, 0, already.clusters.length);
                        System.arraycopy(ca.clusters, 0, ia, already.clusters.length, ca.clusters.length);
                        containerByNodeAddress.put(ca.node, new ContainerAddress(ca.node, ia));
                    } else
                        containerByNodeAddress.put(ca.node, ca);
                }
            }
            // =================================================================================

            for (final Map.Entry<NodeAddress, ContainerAddress> e : containerByNodeAddress.entrySet()) {
                final NodeAddress curNode = e.getKey();
                final ContainerAddress curAddr = e.getValue();

                final RoutedMessage toSend = new RoutedMessage(curAddr.clusters, message.key, message.message);
                if (curNode.equals(thisNode)) {
                    nodeReciever.feedbackLoop(toSend);
                    messageSentSomewhere = true;
                } else {
                    final Sender sf = cur.senderByNode.get(curNode);
                    if (sf == null)
                        LOGGER.error("Couldn't send message to " + curNode + " because there's no " + SenderFactory.class.getSimpleName());
                    else {
                        // TODO: more error handling
                        sf.send(toSend);
                        messageSentSomewhere = true;
                    }
                }
            }

            if (containerByNodeAddress.size() == 0) {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace("There appears to be no valid destination addresses for the message {}",
                            SafeString.objectDescription(message.message));
            }
        } finally {
            if (!messageSentSomewhere)
                statsCollector.messageNotSent();
        }
    }

    @Override
    public void start(final Infrastructure infra) {
        final ClusterInfoSession session = infra.getCollaborator();
        final String nodesDir = infra.getRootPaths().nodesDir;

        checkup = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {
                try {
                    // collect up all NodeInfo's known about.
                    session.recursiveMkdir(nodesDir, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
                    final Collection<String> nodeDirs = session.getSubdirs(nodesDir, this);

                    final Set<NodeInformation> alreadySeen = new HashSet<>();
                    // get all of the subdirectories NodeInformations
                    for (final String subdir : nodeDirs) {
                        final NodeInformation ni = (NodeInformation) session.getData(nodesDir + "/" + subdir, null);

                        if (ni == null) {
                            LOGGER.warn("A node directory was empty at " + subdir);
                            return false;
                        }

                        // see if node info is dupped.
                        if (alreadySeen.contains(ni.nodeAddress)) {
                            LOGGER.warn("The node " + ni.nodeAddress + " seems to be registed more than once.");
                            continue;
                        }

                        if (ni.clusterInfoByClusterId.size() == 0) { // it's ALL adaptor so there's no sense in dealing with it
                            LOGGER.trace("NodeInformation {} appears to be only an Adaptor.", ni);
                            continue;
                        }

                        alreadySeen.add(ni);
                    }

                    // check to see if there's new nodes.
                    final ApplicationState.Update ud = outbounds.get().update(alreadySeen, thisNode);

                    if (!ud.change()) {
                        isReady.set(true);
                        return true; // nothing to update.
                    }

                    // otherwise we will be making changes so remove the current ApplicationState
                    final ApplicationState obs = outbounds.getAndSet(null); // this can cause instability.
                    try {
                        final ApplicationState newState = obs.apply(ud, tmanager, statsCollector, manager);
                        outbounds.set(newState);

                        isReady.set(true);
                        return true;
                    } catch (final RuntimeException rte) {
                        // if we threw an exception after clearing the outbounds we need to restore it.
                        // This is likely a configuration error so we should probably warn about it.
                        LOGGER.warn("Unexpected exception while applying a topology update", rte);
                        outbounds.set(obs);
                        throw rte;
                    }
                } catch (final ClusterInfoException e) {
                    final String message = "Failed to find outgoing route information. Will retry shortly.";
                    if (LOGGER.isTraceEnabled())
                        LOGGER.debug(message, e);
                    else LOGGER.debug(message);
                    return false;
                }
            }

            @Override
            public String toString() {
                return "find nodes to route to";
            }

        };

        outbounds.set(new ApplicationState());

        isRunning.set(true);
        checkup.process();
    }

    @Override
    public boolean isReady() {
        if (isReady.get()) {
            final ApplicationState obs = outbounds.get();
            if (obs == null)
                return false;
            if (!manager.isReady()) // this will check the current Routers.
                return false;
            if (!tmanager.isReady())
                return false;

            // go through all of the outbounds and make sure we know about all of the nodes reachable with the exception
            // of thisNode.
            final Set<NodeAddress> nodes = obs.outboundByClusterName.values().stream()
                    .map(r -> r.allDesintations().stream().map(ca -> ca.node))
                    .flatMap(i -> i)
                    .filter(na -> !na.equals(thisNode)) // make sure it's not thisNode
                    .collect(Collectors.toSet());

            // is there an outbound for each node?
            for (final NodeAddress c : nodes) {
                if (!obs.senderByNode.containsKey(c))
                    return false;
            }
            return true;
        } else return false;
    }

    @Override
    public void stop() {
        synchronized (isRunning) {
            isRunning.set(false);
            stopEm(outbounds.getAndSet(null));
        }
    }

    public static class RoutedMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        public final int[] containers;
        public final Object key;
        public final Object message;

        @SuppressWarnings("unused")
        private RoutedMessage() {
            containers = null;
            key = null;
            message = null;
        }

        public RoutedMessage(final int[] containers, final Object key, final Object message) {
            this.containers = containers;
            this.key = key;
            this.message = message;
        }
    }

    private static void stopEm(final ApplicationState obs) {
        if (obs != null) {
            obs.outboundByClusterName.values().forEach(r -> {
                try {
                    r.release();
                } catch (final RuntimeException rte) {
                    LOGGER.warn("Problem while shutting down an outbound router", rte);
                }
            });

            obs.senderByNode.values().forEach(sf -> {
                try {
                    sf.stop();
                } catch (final RuntimeException rte) {
                    LOGGER.warn("Problem while shutting down an SenderFactory", rte);
                }
            });
        }
    }

    // =====================================================================
    // Strictly for testing.
    // =====================================================================
    boolean canReach(final String cluterName, final KeyedMessageWithType message) {
        final ApplicationState cur = outbounds.get();
        if (cur == null)
            return false;
        final RoutingStrategy.Router ob = cur.outboundByClusterName.get(cluterName);
        if (ob == null)
            return false;
        final ContainerAddress ca = ob.selectDestinationForMessage(message);
        if (ca == null)
            return false;
        return true;
    }

    Collection<ContainerAddress> allReachable(final String cluterName) {
        final ApplicationState cur = outbounds.get();
        if (cur == null)
            return new ArrayList<>();
        final RoutingStrategy.Router ob = cur.outboundByClusterName.get(cluterName);
        if (ob == null)
            return new ArrayList<>();
        return ob.allDesintations();
    }

    NodeAddress thisNode() {
        return thisNode;
    }

    NodeStatsCollector getNodeStatCollector() {
        return statsCollector;
    }
    // =====================================================================

}
