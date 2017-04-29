package net.dempsy.router.managed;

import java.util.concurrent.atomic.AtomicBoolean;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;

public class ManagedInbound implements Inbound {
    private Leader leader;
    private Subscriber subscriber;
    private Utils utils;
    private ClusterId clusterId;
    private ContainerAddress address;
    private KeyspaceChangeListener listener;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener listener) {
        this.clusterId = clusterId;
        this.address = address;
        this.listener = listener;
    }

    @Override
    public void start(final Infrastructure infra) {
        utils = new Utils(infra, clusterId, address);
        // subscriber first because it registers as a node. If there's no nodes
        // there's nothing for the leader to do.
        subscriber = new Subscriber(utils, infra, isRunning, listener, this);
        subscriber.process();
        leader = new Leader(utils, infra, isRunning);
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
        final int shardNum = utils.determineShard(messageKey);
        return subscriber.doIOwnShard(shardNum);
    }
}
