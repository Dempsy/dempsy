package net.dempsy.router.managed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Router;
import net.dempsy.router.managed.Utils.ShardAssignment;
import net.dempsy.util.SafeString;
import net.dempsy.utils.PersistentTask;

public class ManagedRouter extends PersistentTask implements Router {
    private static Logger LOGGER = LoggerFactory.getLogger(ManagedRouter.class);
    private final AtomicReference<ContainerAddress[]> destinations = new AtomicReference<>(null);

    final ClusterId clusterId;
    private final AtomicBoolean isRunning;
    private final Utils utils;
    private final ManagedRouterFactory mommy;
    private final String thisNodeId;

    ManagedRouter(final ManagedRouterFactory mom, final ClusterId clusterId, final Infrastructure infra) {
        super(LOGGER, new AtomicBoolean(true), infra.getScheduler(), 500);
        this.mommy = mom;
        this.clusterId = clusterId;
        this.utils = new Utils(infra, clusterId, null);
        this.thisNodeId = infra.getNodeId();
        this.isRunning = getIsRunningFlag();
        process();
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        final ContainerAddress[] destinations = this.destinations.get();
        if (destinations == null)
            throw new DempsyException("It appears the " + ManagedRouter.class.getSimpleName() + " strategy for the message key " +
                    SafeString.objectDescription(message != null ? message.key : null)
                    + " is being used prior to initialization or after a failure.");

        return destinations[utils.determineShard(message.key)];
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
    public boolean execute() {
        try {
            final List<ShardAssignment> assignments = utils.persistentGetData(utils.shardsAssignedDir, this);
            if (assignments == null)
                return false; // nothing there, try later.

            final ContainerAddress[] newState = new ContainerAddress[utils.totalNumShards];
            for (final ShardAssignment sa : assignments) {
                for (final int index : sa.shards) {
                    if (newState[index] != null)
                        LOGGER.warn("There are 2 nodes that think they both have shard " + index + ". The one that will be used is " + newState[index]
                                + " the one being skipped is " + sa.addr);
                    else
                        newState[index] = sa.addr;
                }
            }
            this.destinations.set(newState);
            return true;
        } catch (final ClusterInfoException cie) {
            throw new RuntimeException(cie);
        }
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
