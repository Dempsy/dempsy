package net.dempsy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import net.dempsy.transport.RoutedMessage;
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
    private final String thisNodeId;
    private final TransportManager tmanager;
    private final NodeReceiver nodeReciever;
    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final NodeStatsCollector statsCollector;

    private static class ApplicationState {
        private final Map<String, RoutingStrategy.Router> outboundByClusterName;
        private final Map<String, List<String>> clusterNameByMessageType;
        private final Map<NodeAddress, NodeInformation> current;
        private final TransportManager tManager;
        private final NodeAddress thisNode;

        private final ConcurrentHashMap<NodeAddress, Sender> senders = new ConcurrentHashMap<>();

        ApplicationState(final Map<String, net.dempsy.router.RoutingStrategy.Router> outboundByClusterName, final Map<String, Set<String>> cnByType,
                final Map<NodeAddress, NodeInformation> current, final TransportManager tManager, final NodeAddress thisNode) {
            this.outboundByClusterName = outboundByClusterName;
            this.clusterNameByMessageType = new HashMap<>();
            cnByType.entrySet().forEach(e -> clusterNameByMessageType.put(e.getKey(), new ArrayList<String>(e.getValue())));
            this.current = current;
            this.tManager = tManager;
            this.thisNode = thisNode;
        }

        ApplicationState(final TransportManager tManager, final NodeAddress thisNode) {
            outboundByClusterName = new HashMap<>();
            clusterNameByMessageType = new HashMap<>();
            current = new HashMap<>();
            this.tManager = tManager;
            this.thisNode = thisNode;
        }

        public static class Update {
            final Set<NodeInformation> toAdd;
            final Set<NodeAddress> toDelete;
            final Set<NodeInformation> leaveAlone;

            public Update(final Set<NodeInformation> leaveAlone, final Set<NodeInformation> toAdd, final Set<NodeAddress> toDelete) {
                this.toAdd = toAdd;
                this.toDelete = toDelete;
                this.leaveAlone = leaveAlone;
            }

            public boolean change() {
                return (!(toDelete.size() == 0 && toAdd.size() == 0));
            }
        }

        public Update update(final Set<NodeInformation> newState, final NodeAddress thisNodeX, final String thisNodeId) {
            final Set<NodeInformation> toAdd = new HashSet<>();
            final Set<NodeAddress> toDelete = new HashSet<>();
            final Set<NodeAddress> knownAddr = new HashSet<>(current.keySet());
            final Set<NodeInformation> leaveAlone = new HashSet<>();

            NodeAddress oursOnTheList = null;

            for (final NodeInformation cur : newState) {
                final NodeInformation known = current.get(cur.nodeAddress);
                if (cur.nodeAddress.equals(thisNodeX))
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

            if (oursOnTheList == null && thisNodeX != null) // we don't seem to have our own address registered with the collaborator.
                                                            // this condition is actually okay since the Router is started before the
                                                            // node is registered with the collaborator.
                LOGGER.trace("Router at {} doesn't seem to have its own address registered with the collaborator yet", thisNodeId);

            // dump the remaining knownAddrs on the toDelete list
            toDelete.addAll(knownAddr);

            return new Update(leaveAlone, toAdd, toDelete);
        }

        public ApplicationState apply(final Update update, final TransportManager tmanager, final NodeStatsCollector statsCollector,
                final RoutingStrategyManager manager) {
            // apply toDelete first.
            final Set<NodeAddress> toDelete = update.toDelete;

            if (toDelete.size() > 0) { // just clear all senders.
                for (final NodeAddress a : toDelete) {
                    final Sender s = senders.get(a);
                    if (s != null)
                        s.stop();
                }
            }

            final Map<NodeAddress, NodeInformation> newCurrent = new HashMap<>();

            // the one's to carry over.
            final Set<NodeInformation> leaveAlone = update.leaveAlone;
            for (final NodeInformation cur : leaveAlone) {
                newCurrent.put(cur.nodeAddress, cur);
            }

            // add new senders
            final Set<NodeInformation> toAdd = update.toAdd;
            for (final NodeInformation cur : toAdd) {
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

            return new ApplicationState(newOutboundByClusterName, cnByType, newCurrent, tmanager, thisNode);
        }

        public void stop() {
            outboundByClusterName.values().forEach(r -> {
                try {
                    r.release();
                } catch (final RuntimeException rte) {
                    LOGGER.warn("Problem while shutting down an outbound router", rte);
                }
            });

            final List<Sender> tmps = new ArrayList<>();

            // keep removing
            while (senders.size() > 0)
                senders.keySet().forEach(k -> tmps.add(senders.remove(k)));

            tmps.forEach(s -> s.stop());
        }

        public Sender getSender(final NodeAddress na) {
            final Sender ret = senders.get(na);
            if (ret == null) {
                if (na.equals(thisNode))
                    return null;
                return senders.computeIfAbsent(na, n -> {
                    final SenderFactory sf = tManager.getAssociatedInstance(na.getClass().getPackage().getName());
                    return sf.getSender(n);
                });
            }
            return ret;
        }

    }

    public Router(final RoutingStrategyManager manager, final NodeAddress thisNode, final String thisNodeId, final NodeReceiver nodeReciever,
            final TransportManager tmanager, final NodeStatsCollector statsCollector) {
        this.manager = manager;
        this.thisNode = thisNode;
        this.thisNodeId = thisNodeId;
        this.tmanager = tmanager;
        this.nodeReciever = nodeReciever;
        this.statsCollector = statsCollector;
    }

    @Override
    public void dispatch(final KeyedMessageWithType message) {
        boolean messageSentSomewhere = false;
        try {
            ApplicationState tmp = outbounds.get();

            // if we're in the midst of an update then we really want to wait for the new state.
            while (tmp == null) {
                if (!isRunning.get()) {
                    LOGGER.debug("Router dispatch called while stopped.");
                    return;
                }

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
                    LOGGER.debug("No way to send the message {} to the cluster {} for the time being", message.message, clusterName);
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
                    nodeReciever.feedbackLoop(toSend, false); // this shouldn't count since Router is an OUTGOING class
                    messageSentSomewhere = true;
                } else {
                    final Sender sf = cur.getSender(curNode);
                    if (sf == null) {
                        // router update is probably behind the routing strategy update
                        if (isRunning.get())
                            LOGGER.error("Couldn't send message to " + curNode + " from " + thisNodeId + " because there's no "
                                    + Sender.class.getSimpleName());
                    } else {
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
                    final ApplicationState.Update ud = outbounds.get().update(alreadySeen, thisNode, thisNodeId);

                    if (!ud.change()) {
                        isReady.set(true);
                        return true; // nothing to update.
                    } else if (LOGGER.isTraceEnabled())
                        LOGGER.trace("Updating for " + thisNodeId);

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

        outbounds.set(new ApplicationState(tmanager, thisNode));

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

            return true;
        } else return false;
    }

    @Override
    public void stop() {
        synchronized (isRunning) {
            isRunning.set(false);
            final ApplicationState cur = outbounds.getAndSet(null);
            if (cur != null)
                cur.stop();
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

    String thisNodeId() {
        return thisNodeId;
    }

    NodeStatsCollector getNodeStatCollector() {
        return statsCollector;
    }
    // =====================================================================

}
