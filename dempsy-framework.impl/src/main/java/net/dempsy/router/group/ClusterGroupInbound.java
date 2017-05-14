package net.dempsy.router.group;

import static net.dempsy.router.group.intern.GroupUtils.groupNameFromTypeIdDontThrowNoColon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.router.group.intern.GroupDetails;
import net.dempsy.router.shardutils.Leader;
import net.dempsy.router.shardutils.Subscriber;
import net.dempsy.router.shardutils.Utils;
import net.dempsy.transport.NodeAddress;

public class ClusterGroupInbound {
    private final static Logger LOGGER = LoggerFactory.getLogger(ClusterGroupInbound.class);
    private Leader<GroupDetails> leader;
    private Subscriber<GroupDetails> subscriber;
    private Utils<GroupDetails> utils;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private int mask = 0;

    private final List<Proxy> inbounds = new ArrayList<>();
    private boolean started = false;
    private GroupDetails groupDetails = null;
    private Map<String, ContainerAddress> caByCluster = null;

    private static final Map<NodeAddress, Map<String, ClusterGroupInbound>> current = new HashMap<>();

    public static class Proxy implements Inbound {
        ClusterGroupInbound proxied = null;

        private ClusterId clusterId = null;
        private ContainerAddress address = null;
        private KeyspaceChangeListener listener = null;
        private boolean stopped = false;
        private String groupName = null;

        private Subscriber<GroupDetails> subscriber = null;
        private Utils<GroupDetails> utils = null;
        private int mask = 0;

        @Override
        public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener listener) {
            this.clusterId = clusterId;
            this.address = address;
            this.listener = listener;

            if (groupName == null) {
                LOGGER.warn("No group specified for cluster group inbound for " + clusterId + " at " + address.node
                        + " using the cluster name. You should choose a different routing strategy.");
                groupName = clusterId.clusterName;
            }

            proxied = get(groupName, address.node);
            proxied.addContainerDetails(this);
        }

        @Override
        public void typeId(final String typeId) {
            groupName = groupNameFromTypeIdDontThrowNoColon(typeId);
        }

        @Override
        public void start(final Infrastructure infra) {
            proxied.maybeStart(infra, this);
        }

        @Override
        public synchronized void stop() {
            if (!stopped) {
                proxied.stopMe(this);
                stopped = true;
            }
        }

        @Override
        public boolean isReady() {
            return proxied.isReady();
        }

        @Override
        public boolean doesMessageKeyBelongToNode(final Object messageKey) {
            final int shardNum = utils.determineShard(messageKey, mask);
            return subscriber.doIOwnShard(shardNum);
        }

        // called back from ClusterGroupInbound once it's fully started.
        private void setup(final Subscriber<GroupDetails> subscriber, final Utils<GroupDetails> utils, final int mask) {
            this.mask = mask;
            this.subscriber = subscriber;
            this.utils = utils;
        }
    }

    private static ClusterGroupInbound get(final String groupName, final NodeAddress nodeAddress) {
        Map<String, ClusterGroupInbound> map;
        synchronized (current) { // this isn't really necessary but wont hurt since start is only called once.
            map = current.get(nodeAddress);
            if (map == null) {
                map = new HashMap<String, ClusterGroupInbound>();
                current.put(nodeAddress, map);
            }
        }

        ClusterGroupInbound proxied;
        synchronized (map) {
            proxied = map.get(groupName);
            if (proxied == null) {
                proxied = new ClusterGroupInbound();
                map.put(groupName, proxied);
            }
        }

        return proxied;
    }

    private void addContainerDetails(final Proxy ib) {
        inbounds.add(ib);
    }

    private int numStarted = 0;

    private synchronized void maybeStart(final Infrastructure infra, final Proxy ib) {

        if (started) // this should not be possible
            throw new IllegalStateException("start() called on a group routing strategy more times than there are clusters in the group.");

        if (ib.groupName == null)
            throw new IllegalStateException("The group name isn't set on the inbound for " + ib.clusterId
                    + ". This shouldn't be possible. Was the typeId specified correctly?");

        if (groupDetails == null) {
            groupDetails = new GroupDetails(ib.groupName, ib.address.node);
            caByCluster = new HashMap<>();
        } else if (!groupDetails.groupName.equals(ib.groupName))
            throw new IllegalStateException("The group name for " + ib.clusterId + " is " + ib.groupName
                    + " but doesn't match prevous group names supposedly in the same group: " + groupDetails.groupName);
        else if (!groupDetails.node.equals(ib.address.node))
            throw new IllegalStateException("The node address for " + ib.clusterId + " is " + ib.address.node
                    + " but doesn't match prevous group names supposedly in the same group: " + groupDetails.node);

        if (caByCluster.containsKey(ib.clusterId.clusterName))
            throw new IllegalStateException("There appears to be two inbounds both configured with the same cluster id:" + ib.clusterId);

        caByCluster.put(ib.clusterId.clusterName, ib.address);

        if (!inbounds.contains(ib))
            throw new IllegalStateException(
                    "Not all routing strategies that are part of this group seemed to have had setContainerDetails called prior to start.");

        numStarted++;
        if (numStarted == inbounds.size()) { // if all of the proxies have checked in then we need to go ahead and start
            started = true;

            final int totalShards = Integer
                    .parseInt(infra.getConfigValue(ClusterGroupInbound.class, Utils.CONFIG_KEY_TOTAL_SHARDS, Utils.DEFAULT_TOTAL_SHARDS));
            final int minNodes = Integer
                    .parseInt(infra.getConfigValue(ClusterGroupInbound.class, Utils.CONFIG_KEY_MIN_NODES, Utils.DEFAULT_MIN_NODES));

            if (Integer.bitCount(totalShards) != 1)
                throw new IllegalArgumentException("The configuration property \"" + Utils.CONFIG_KEY_TOTAL_SHARDS
                        + "\" must be set to a power of 2. It's currently set to " + totalShards);

            this.mask = totalShards - 1;

            groupDetails.fillout(caByCluster);

            utils = new Utils<GroupDetails>(infra, groupDetails.groupName, groupDetails);
            // subscriber first because it registers as a node. If there's no nodes
            // there's nothing for the leader to do.
            subscriber = new Subscriber<GroupDetails>(utils, infra, isRunning,
                    new CompoundKeyspaceListener(inbounds.stream().map(p -> p.listener).collect(Collectors.toList())), totalShards);
            subscriber.process();
            leader = new Leader<GroupDetails>(utils, totalShards, minNodes, infra, isRunning, GroupDetails[]::new);
            leader.process();

            inbounds.forEach(p -> p.setup(subscriber, utils, mask));
        }
    }

    private void stopMe(final Proxy ib) {
        if (!inbounds.remove(ib)) {
            throw new IllegalStateException(
                    "Attempt to remove cluster inbound for " + ib.clusterId + " from group where it's not there. Was it stopped twice?");
        }

        if (inbounds.size() == 0) {
            // remove me from the list
            if (groupDetails != null) {
                final Map<String, ClusterGroupInbound> map;
                synchronized (current) {
                    map = current.get(groupDetails.node);
                }

                if (map != null) {
                    synchronized (map) {
                        map.remove(groupDetails.groupName);
                    }
                }
            }
            isRunning.set(false);
        }
    }

    private boolean isReady() {
        return leader.isReady() && subscriber.isReady();
    }

    private static class CompoundKeyspaceListener implements KeyspaceChangeListener {
        public final List<KeyspaceChangeListener> listeners;

        CompoundKeyspaceListener(final List<KeyspaceChangeListener> listeners) {
            this.listeners = new ArrayList<>(listeners);
        }

        @Override
        public void keyspaceChanged(final boolean less, final boolean more) {
            listeners.forEach(l -> l.keyspaceChanged(less, more));
        }
    }

}
