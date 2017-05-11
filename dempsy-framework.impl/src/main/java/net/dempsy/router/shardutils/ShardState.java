package net.dempsy.router.shardutils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.router.shardutils.Utils.ShardAssignment;
import net.dempsy.utils.PersistentTask;

public class ShardState<C> extends PersistentTask {
    private static Logger LOGGER = LoggerFactory.getLogger(ShardState.class);
    private final AtomicReference<C[]> destinations = new AtomicReference<>(null);

    private final Utils<C> utils;
    private final String thisNodeId;
    private final IntFunction<C[]> newArraySupplier;
    private final String groupName;
    private final IntConsumer setMask;
    private int mask = 0;

    public ShardState(final String groupName, final Infrastructure infra, final AtomicBoolean isRunning, final IntFunction<C[]> newArraySupplier,
            final IntConsumer setMask) {
        super(LOGGER, isRunning, infra.getScheduler(), 500);
        this.groupName = groupName;
        this.utils = new Utils<C>(infra, groupName, null);
        this.thisNodeId = infra.getNodeId();
        this.newArraySupplier = newArraySupplier;
        this.setMask = setMask;
        process();
    }

    public AtomicReference<C[]> getShardContentsArray() {
        return destinations;
    }

    public Utils<C> getUtils() {
        return utils;
    }

    @Override
    public boolean execute() {
        try {
            final List<ShardAssignment<C>> assignments = utils.persistentGetData(utils.shardsAssignedDir, this);
            if (assignments == null || assignments.size() < 1)
                return false; // nothing there, try later.

            final ShardAssignment<C> first = assignments.get(0);
            final int totalShardCount = first.totalNumShards;

            if (Integer.bitCount(totalShardCount) != 1) {
                final String message = "The total number of shards that was stored as the cluster information for " + groupName
                        + " is not a power of '2'. This shouldn't be possible unless some nefarious actor hacked the repository. The Russians must be everywhere. Call Rachel Madow! Cannot continue.";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }

            if (mask == 0) {
                mask = totalShardCount - 1;
                setMask.accept(mask);
            } else {
                if (mask != totalShardCount - 1) {
                    final String message = "The cluster group " + groupName
                            + " is not consistently configured. The total shard count seems to have varying values including (at least) " + (mask + 1)
                            + " and " + totalShardCount;
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }
            }

            final C[] newState = newArraySupplier.apply(totalShardCount);
            for (final ShardAssignment<C> sa : assignments) {
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
        return "{" + ShardState.class.getSimpleName() + " at " + thisNodeId + " to " + groupName + "}";
    }

    /**
     * This makes sure all of the destinations are full.
     */
    boolean isReady() {
        final C[] ds = destinations.get();
        if (ds == null)
            return false;
        for (final C d : ds)
            if (d == null)
                return false;
        final boolean ret = ds.length != 0; // this method is only called in tests and this needs to be true there.

        if (ret && LOGGER.isDebugEnabled())
            LOGGER.debug("at {} to {} is Ready " + shorthand(ds), thisNodeId, groupName);

        return ret;
    }

    private static final <C> Set<C> shorthand(final C[] addr) {
        if (addr == null)
            return null;
        return Arrays.stream(addr).collect(Collectors.toSet());
    }

}
