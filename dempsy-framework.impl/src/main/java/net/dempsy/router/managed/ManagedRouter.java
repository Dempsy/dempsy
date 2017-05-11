package net.dempsy.router.managed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Router;
import net.dempsy.router.shardutils.ShardState;
import net.dempsy.router.shardutils.Utils;
import net.dempsy.util.SafeString;

public class ManagedRouter implements Router, IntConsumer {
    private static Logger LOGGER = LoggerFactory.getLogger(ManagedRouter.class);
    private final AtomicReference<ContainerAddress[]> destinations;

    final ClusterId clusterId;
    private final AtomicBoolean isRunning;
    private final ManagedRouterFactory mommy;
    private final String thisNodeId;

    private final ShardState<ContainerAddress> state;
    private final Utils<ContainerAddress> utils;
    private int mask = 0;

    ManagedRouter(final ManagedRouterFactory mom, final ClusterId clusterId, final Infrastructure infra) {
        this.mommy = mom;
        this.clusterId = clusterId;
        this.thisNodeId = infra.getNodeId();
        this.isRunning = new AtomicBoolean(true);
        this.state = new ShardState<ContainerAddress>(clusterId.clusterName, infra, isRunning, ContainerAddress[]::new, this);
        this.utils = state.getUtils();
        this.destinations = state.getShardContentsArray();
        this.state.process();
    }

    @Override
    public void accept(final int mask) {
        this.mask = mask;
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        final ContainerAddress[] destinations = this.destinations.get();
        if (destinations == null)
            throw new DempsyException("It appears the " + ManagedRouter.class.getSimpleName() + " strategy for the message key " +
                    SafeString.objectDescription(message != null ? message.key : null)
                    + " is being used prior to initialization or after a failure.");

        return destinations[utils.determineShard(message.key, mask)];
    }

    @Override
    public Collection<ContainerAddress> allDesintations() {
        final ContainerAddress[] cur = destinations.get();
        if (cur == null)
            return new ArrayList<>();
        return new ArrayList<>(Arrays.stream(cur).filter(ca -> ca != null).collect(Collectors.toSet()));
    }

    @Override
    public synchronized void release() {
        mommy.release(this);
        isRunning.set(false);
    }

    @Override
    public String toString() {
        return "{" + ManagedRouter.class.getSimpleName() + " at " + thisNodeId + " to " + clusterId + "}";
    }

    /**
     * This makes sure all of the destinations are full.
     */
    boolean isReady() {
        final ContainerAddress[] ds = destinations.get();
        if (ds == null)
            return false;
        for (final ContainerAddress d : ds)
            if (d == null)
                return false;
        final boolean ret = ds.length != 0; // this method is only called in tests and this needs to be true there.

        if (ret && LOGGER.isDebugEnabled())
            LOGGER.debug("at {} to {} is Ready " + shorthand(ds), thisNodeId, clusterId);

        return ret;
    }

    private static final Set<ContainerAddress> shorthand(final ContainerAddress[] addr) {
        if (addr == null)
            return null;
        return Arrays.stream(addr).collect(Collectors.toSet());
    }

}
