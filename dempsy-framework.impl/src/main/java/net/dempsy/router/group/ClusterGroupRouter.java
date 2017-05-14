package net.dempsy.router.group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import net.dempsy.router.group.intern.GroupDetails;
import net.dempsy.router.shardutils.ShardState;
import net.dempsy.router.shardutils.Utils;
import net.dempsy.util.SafeString;

public class ClusterGroupRouter implements Router, IntConsumer {
    private static Logger LOGGER = LoggerFactory.getLogger(ClusterGroupRouter.class);
    private final AtomicReference<GroupDetails[]> destinations;

    final ClusterId clusterId;
    private final String clusterName;
    private final AtomicBoolean isRunning;
    private final ClusterGroupRouterFactory mommy;
    private final String thisNodeId;

    private final ShardState<GroupDetails> state;
    private final Utils<GroupDetails> utils;
    private int mask = 0;
    private int containerIndex = -1;

    ClusterGroupRouter(final ClusterGroupRouterFactory mom, final ClusterId clusterId, final Infrastructure infra, final String groupName) {
        this.mommy = mom;
        this.clusterId = clusterId;
        this.clusterName = clusterId.clusterName;
        this.thisNodeId = infra.getNodeId();
        this.isRunning = new AtomicBoolean(true);
        this.state = new ShardState<GroupDetails>(groupName, infra, isRunning, GroupDetails[]::new, this);
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
        final GroupDetails[] destinations = this.destinations.get();
        if (destinations == null)
            throw new DempsyException("It appears the " + ClusterGroupRouter.class.getSimpleName() + " strategy for the message key " +
                    SafeString.objectDescription(message != null ? message.key : null)
                    + " is being used prior to initialization or after a failure.");
        if (containerIndex < 0) {
            containerIndex = getIndex(destinations, clusterName);
            if (containerIndex < 0)
                return null;
        }

        final GroupDetails cur = destinations[utils.determineShard(message.key, mask)];
        return cur.containerAddresses[containerIndex];
    }

    @Override
    public Collection<ContainerAddress> allDesintations() {
        final GroupDetails[] cur = destinations.get();
        if (containerIndex < 0) {
            containerIndex = getIndex(cur, clusterName);
            if (containerIndex < 0)
                return Collections.emptyList();
        }
        if (cur == null)
            return new ArrayList<>();
        return new ArrayList<>(Arrays.stream(cur)
                .filter(gd -> gd != null)
                .map(gd -> gd.containerAddresses[containerIndex])
                .filter(ca -> ca != null)
                .collect(Collectors.toSet()));
    }

    @Override
    public synchronized void release() {
        mommy.release(this);
        isRunning.set(false);
    }

    @Override
    public String toString() {
        return "{" + ClusterGroupRouter.class.getSimpleName() + " at " + thisNodeId + " to " + clusterId + "}";
    }

    /**
     * This makes sure all of the destinations are full.
     */
    boolean isReady() {
        final GroupDetails[] ds = destinations.get();
        if (ds == null)
            return false;
        for (final GroupDetails d : ds)
            if (d == null)
                return false;

        final boolean ret = ds.length != 0 && getIndex(ds, clusterName) >= 0; // this method is only called in tests and this needs to be true there.

        if (ret && LOGGER.isDebugEnabled())
            LOGGER.debug("at {} to {} is Ready " + shorthand(cvrt(ds, clusterName)), thisNodeId, clusterId);

        return ret;
    }

    private static int getIndex(final GroupDetails[] destinations, final String clustername) {
        final GroupDetails gd = Arrays.stream(destinations).filter(g -> g != null).findAny().orElse(null);
        if (gd == null)
            return -1;

        final Integer ret = gd.clusterIndicies.get(clustername);
        return ret == null ? -1 : ret.intValue();
    }

    private static ContainerAddress[] cvrt(final GroupDetails[] gds, final String clusterName) {
        final int clusterIndex = getIndex(gds, clusterName);
        if (clusterIndex < 0)
            return new ContainerAddress[0];

        return Arrays.stream(gds)
                .map(gd -> gd == null ? null : gd.containerAddresses[clusterIndex])
                .toArray(ContainerAddress[]::new);
    }

    private static final Set<ContainerAddress> shorthand(final ContainerAddress[] addr) {
        if (addr == null)
            return null;
        return Arrays.stream(addr).collect(Collectors.toSet());
    }

}
