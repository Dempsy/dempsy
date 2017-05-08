package net.dempsy.intern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.dempsy.ClusterInformation;
import net.dempsy.NodeInformation;
import net.dempsy.OutgoingDispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.TransportManager;

public class ApplicationState {
    public final Map<String, RoutingStrategy.Router[]> outboundsByMessageType;

    private final Map<String, RoutingStrategy.Router> outboundByClusterName_;
    private final Map<String, List<String>> clusterNameByMessageType;
    private final Map<NodeAddress, NodeInformation> current;
    private final TransportManager tManager;
    private final NodeAddress thisNode;

    private final ConcurrentHashMap<NodeAddress, Sender> senders = new ConcurrentHashMap<>();

    private ApplicationState(final Map<String, RoutingStrategy.Router> outboundByClusterName, final Map<String, Set<String>> cnByType,
            final Map<NodeAddress, NodeInformation> current, final TransportManager tManager, final NodeAddress thisNode) {
        this.outboundByClusterName_ = outboundByClusterName;
        this.clusterNameByMessageType = new HashMap<>();
        cnByType.entrySet().forEach(e -> clusterNameByMessageType.put(e.getKey(), new ArrayList<String>(e.getValue())));
        this.current = current;
        this.tManager = tManager;
        this.thisNode = thisNode;

        outboundsByMessageType = new HashMap<>();
        final HashMap<String, Set<RoutingStrategy.Router>> tmp = new HashMap<>();
        for (final Map.Entry<String, Set<String>> e : cnByType.entrySet()) {
            final Set<String> clusterNames = e.getValue();
            final String messageType = e.getKey();
            final Set<RoutingStrategy.Router> cur = tmp.computeIfAbsent(messageType, k -> new HashSet<>());
            for (final String clusterName : clusterNames) {
                final RoutingStrategy.Router router = outboundByClusterName.get(clusterName);
                if (router != null)
                    cur.add(router);
            }
        }

        for (final Map.Entry<String, Set<RoutingStrategy.Router>> e : tmp.entrySet())
            outboundsByMessageType.put(e.getKey(), e.getValue().stream().toArray(RoutingStrategy.Router[]::new));
    }

    public ApplicationState(final TransportManager tManager, final NodeAddress thisNode) {
        outboundByClusterName_ = new HashMap<>();
        outboundsByMessageType = new HashMap<>();
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

    public ApplicationState.Update update(final Set<NodeInformation> newState, final NodeAddress thisNodeX, final String thisNodeId) {
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
            OutgoingDispatcher.LOGGER.trace("Router at {} doesn't seem to have its own address registered with the collaborator yet", thisNodeId);

        // dump the remaining knownAddrs on the toDelete list
        toDelete.addAll(knownAddr);

        return new Update(leaveAlone, toAdd, toDelete);
    }

    public ApplicationState apply(final ApplicationState.Update update, final TransportManager tmanager, final NodeStatsCollector statsCollector,
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

        final Set<String> knownClusterOutbounds = new HashSet<>(outboundByClusterName_.keySet());

        for (final ClusterInformation ci : allCis) {
            final String clusterName = ci.clusterId.clusterName;
            final RoutingStrategy.Router ob = outboundByClusterName_.get(clusterName);
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
        outboundByClusterName_.values().forEach(r -> {
            try {
                r.release();
            } catch (final RuntimeException rte) {
                OutgoingDispatcher.LOGGER.warn("Problem while shutting down an outbound router", rte);
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

    // =====================================================================
    // Strictly for testing.
    // =====================================================================
    public boolean canReach(final String cluterName, final KeyedMessageWithType message) {
        final RoutingStrategy.Router ob = outboundByClusterName_.get(cluterName);
        if (ob == null)
            return false;
        final ContainerAddress ca = ob.selectDestinationForMessage(message);
        if (ca == null)
            return false;
        return true;
    }

    public Collection<ContainerAddress> allReachable(final String cluterName) {
        final RoutingStrategy.Router ob = outboundByClusterName_.get(cluterName);
        if (ob == null)
            return new ArrayList<>();
        return ob.allDesintations();
    }

}