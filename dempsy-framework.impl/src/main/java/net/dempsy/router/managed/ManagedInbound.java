package net.dempsy.router.managed;

import java.util.concurrent.atomic.AtomicBoolean;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.router.shardutils.Leader;
import net.dempsy.router.shardutils.Subscriber;
import net.dempsy.router.shardutils.Utils;

public class ManagedInbound implements Inbound {
    private Leader<ContainerAddress> leader;
    private Subscriber<ContainerAddress> subscriber;
    private Utils<ContainerAddress> utils;
    private ClusterId clusterId;
    private ContainerAddress address;
    private KeyspaceChangeListener listener;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private int mask = 0;

    @Override
    public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener listener) {
        this.clusterId = clusterId;
        this.address = address;
        this.listener = listener;
    }

    @Override
    public void start(final Infrastructure infra) {
        final int totalShards = Integer
                .parseInt(infra.getConfigValue(ManagedInbound.class, Utils.CONFIG_KEY_TOTAL_SHARDS, Utils.DEFAULT_TOTAL_SHARDS));
        final int minNodes = Integer.parseInt(infra.getConfigValue(ManagedInbound.class, Utils.CONFIG_KEY_MIN_NODES, Utils.DEFAULT_MIN_NODES));

        if (Integer.bitCount(totalShards) != 1)
            throw new IllegalArgumentException("The configuration property \"" + Utils.CONFIG_KEY_TOTAL_SHARDS
                    + "\" must be set to a power of 2. It's currently set to " + totalShards);

        this.mask = totalShards - 1;

        utils = new Utils<ContainerAddress>(infra, clusterId.clusterName, address);
        // subscriber first because it registers as a node. If there's no nodes
        // there's nothing for the leader to do.
        subscriber = new Subscriber<ContainerAddress>(utils, infra, isRunning, listener, totalShards);
        subscriber.process();
        leader = new Leader<ContainerAddress>(utils, totalShards, minNodes, infra, isRunning, ContainerAddress[]::new);
        leader.process();
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

    @Override
    public boolean isReady() {
        return leader.isReady() && subscriber.isReady();
    }

    @Override
    public boolean doesMessageKeyBelongToNode(final Object messageKey) {
        final int shardNum = utils.determineShard(messageKey, mask);
        return subscriber.doIOwnShard(shardNum);
    }
}
