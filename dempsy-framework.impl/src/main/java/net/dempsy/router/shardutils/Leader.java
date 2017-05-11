package net.dempsy.router.shardutils;

import static net.dempsy.util.Functional.chain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.router.shardutils.Utils.ShardAssignment;
import net.dempsy.router.shardutils.Utils.SubdirAndData;
import net.dempsy.utils.PersistentTask;

public class Leader<C> extends PersistentTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

    private boolean imIt = false;
    private final Utils<C> utils;
    private final ClusterInfoSession session;
    private final PersistentTask nodesChangedTask;
    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final IntFunction<C[]> newArraySupplier;
    private final int totalNumShards;
    private final int minNodes;

    public Leader(final Utils<C> msutils, final int totalNumShards, final int minNodes, final Infrastructure infra, final AtomicBoolean isRunning,
            final IntFunction<C[]> newArraySupplier) {
        super(LOGGER, isRunning, infra.getScheduler(), 500);
        this.utils = msutils;
        this.session = utils.session;
        this.newArraySupplier = newArraySupplier;
        this.totalNumShards = totalNumShards;
        this.minNodes = minNodes;

        if (Integer.bitCount(totalNumShards) != 1)
            throw new IllegalArgumentException("The configuration property \"" + Utils.CONFIG_KEY_TOTAL_SHARDS
                    + "\" must be set to a power of 2. It's currently set to " + totalNumShards);

        this.nodesChangedTask = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), 500) {
            @Override
            public boolean execute() {
                try {
                    return nodesChanged();
                } catch (final ClusterInfoException cie) {
                    throw new RuntimeException(cie);
                }
            }
        };
    }

    @Override
    public boolean execute() {
        try {
            // ====================================================================
            // This will determine if I'm it.
            if (imIt) {
                // if I'm it already, then let's make sure I'm still it.
                imIt = registerAndConfirmIfImIt();
            } else {
                // just grab in an attempt to be it.
                session.recursiveMkdir(utils.masterDetermineDir, utils.thisNodeAddress, DirMode.PERSISTENT, DirMode.EPHEMERAL);
                imIt = registerAndConfirmIfImIt();
            }
            // so, now we know. imIt should be set
            // ====================================================================

            if (imIt) {
                if (!nodesChanged())
                    return false;
            }

            isReady.set(true);
            return true;
        } catch (final ClusterInfoException cie) {
            // TODO: fixme
            throw new RuntimeException(cie);
        }
    }

    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public String toString() {
        return "try to become leader of " + utils.leaderDir + (imIt ? " and I'm it." : " and I'm not it.");
    }

    // =======================================================================
    // Test Access
    // =======================================================================

    boolean imIt() {
        return imIt;
    }

    // =======================================================================

    private boolean nodesChanged() throws ClusterInfoException {
        if (!imIt) {
            LOGGER.warn("Was notified of a nodes change but I'm not the master.");
            return true;
        } else
            LOGGER.trace("Master was notifed of node changes");

        // current nodes registered, sorted by rank
        final List<SubdirAndData<C>> currentNodes = chain(
                // I need to be notified when Nodes appear or disappear v
                utils.persistentGetSubdirAndData(utils.nodesDir, nodesChangedTask, null), p -> Utils.rankSort(p));

        // get all of the current shard assignments
        @SuppressWarnings("unchecked")
        final List<ShardAssignment<C>> assignments = Optional
                .ofNullable((List<ShardAssignment<C>>) utils.persistentGetData(utils.shardsAssignedDir, null))
                .orElse(new ArrayList<>());

        // Build a lookup to determine the rank of a given node denoted by its ContainerAddress
        final Map<C, Integer> rankByCa = new HashMap<>();
        for (int i = 0; i < currentNodes.size(); i++)
            rankByCa.put(currentNodes.get(i).data, Integer.valueOf(i));

        // create an array of already assigned and accepted addresses
        final C[] assignedTo = newArraySupplier.apply(totalNumShards);
        for (final ShardAssignment<C> sa : assignments) {
            // do we know about this destination?
            if (rankByCa.get(sa.addr) != null) {
                for (final int shard : sa.shards) {
                    if (assignedTo[shard] == null)
                        assignedTo[shard] = sa.addr;
                    else
                        // the shard is assigned twice.
                        LOGGER.warn("Shard " + shard + " is assigned to 2 nodes. " + assignedTo[shard] + " and " + sa.addr);
                }
            } else {
                LOGGER.info("The node " + sa.addr + " seems to have dissapeared.");
            }
        }

        // for each node in order I need to build a reduced state.
        for (int i = currentNodes.size() - 1; i >= 0; i--) {
            final C cur = currentNodes.get(i).data;

            final Set<Integer> shardsToRelease = perNodeRelease(cur, assignedTo, currentNodes.size(), i);
            for (final Integer shard : shardsToRelease) {
                assignedTo[shard] = null;
            }
        }

        // now go through and add
        for (int i = 0; i < currentNodes.size(); i++) {
            final C cur = currentNodes.get(i).data;
            final Set<Integer> shardsToAdd = perNodeAcquire(cur, assignedTo, currentNodes.size(), i);
            for (final Integer shard : shardsToAdd) {
                assignedTo[shard] = cur;
            }
        }

        // now write the results.
        final Map<C, List<Integer>> tmp = new HashMap<>();
        for (int i = 0; i < assignedTo.length; i++) {
            final C cur = assignedTo[i];
            if (cur != null) {
                List<Integer> shards = tmp.get(cur);
                if (shards == null) {
                    shards = new ArrayList<>();
                    tmp.put(cur, shards);
                }
                shards.add(Integer.valueOf(i));
            }
        }

        // now create the list of new assignments.
        final List<ShardAssignment<C>> newAssignments = new ArrayList<>(tmp.entrySet().stream()
                .map(e -> new ShardAssignment<C>(e.getValue().stream().mapToInt(i -> i.intValue()).toArray(), e.getKey(), totalNumShards, minNodes))
                .collect(Collectors.toList()));

        session.setData(utils.shardsAssignedDir, newAssignments);
        return true;
    }

    private static <C> List<Integer> buildDestinationsAcquired(final C thisNodeAddress, final C[] currentState) {
        // destinationsAcquired reflects what we already have according to the currentState
        return new ArrayList<>(IntStream.range(0, currentState.length)
                .filter(i -> thisNodeAddress.equals(currentState[i]))
                .mapToObj(i -> Integer.valueOf(i))
                .collect(Collectors.toSet()));
    }

    // go through and determine which nodes to give up ... if any
    private Set<Integer> perNodeRelease(final C thisNodeAddress, final C[] currentState, final int nodeCount, final int nodeRank) {
        final int numberIShouldHave = howManyShouldIHave(totalNumShards, nodeCount, minNodes, nodeRank);

        // destinationsAcquired reflects what we already have according to the currentState
        final List<Integer> destinationsAcquired = buildDestinationsAcquired(thisNodeAddress, currentState);

        final int numHad = destinationsAcquired.size();
        final Set<Integer> ret = new HashSet<>();
        if (destinationsAcquired.size() > numberIShouldHave) { // there's some to remove.
            final Random random = new Random();

            while (destinationsAcquired.size() > numberIShouldHave) {
                final int index = random.nextInt(destinationsAcquired.size());
                ret.add(destinationsAcquired.remove(index));
            }
        }

        if (LOGGER.isTraceEnabled() && ret.size() > 0)
            LOGGER.trace(thisNodeAddress.toString() + " removing shards " + ret + " because I have " +
                    numHad + " but shouldn't have more than " + numberIShouldHave + ".");

        return ret;
    }

    private Set<Integer> perNodeAcquire(final C thisNodeAddress, final C[] currentState, final int nodeCount, final int nodeRank) {
        final int numberIShouldHave = howManyShouldIHave(totalNumShards, nodeCount, minNodes, nodeRank);

        // destinationsAcquired reflects what we already have according to the currentState
        final List<Integer> destinationsAcquired = buildDestinationsAcquired(thisNodeAddress, currentState);

        final int numHad = destinationsAcquired.size();

        final Set<Integer> ret = new HashSet<>();
        if (destinationsAcquired.size() < numberIShouldHave) {
            final int numICanAdd = numberIShouldHave - destinationsAcquired.size();

            final Random random = new Random();

            final List<Integer> shardsAvailable = new ArrayList<>(IntStream.range(0, currentState.length)
                    .filter(i -> currentState[i] == null)
                    .mapToObj(i -> Integer.valueOf(i))
                    .collect(Collectors.toSet()));

            while (shardsAvailable.size() > 0 && ret.size() < numICanAdd) {
                final int index = random.nextInt(shardsAvailable.size());
                ret.add(shardsAvailable.remove(index));
            }
        }

        if (LOGGER.isTraceEnabled() && ret.size() > 0)
            LOGGER.trace(thisNodeAddress.toString() + " adding shards " + ret + " because I have " +
                    numHad + " but should have " + numberIShouldHave + ".");

        return ret;
    }

    private final static int howManyShouldIHave(final int totalShardCount, final int numNodes, final int minNodes, final int myRank) {
        final int numNodesToConsider = Math.max(numNodes, minNodes);
        final int base = Math.floorDiv(totalShardCount, numNodesToConsider);
        final int mod = Math.floorMod(totalShardCount, numNodesToConsider);
        return myRank < mod ? (base + 1) : base;
    }

    // Register to listen for changes on the manage directory and also figure out
    // whether or not imIt and return that.
    private boolean registerAndConfirmIfImIt() throws ClusterInfoException {
        // reset the subdir watcher
        final Collection<String> imItSubdirs = utils.persistentGetSubdir(utils.leaderDir, this);

        // "there can be only one"
        if (imItSubdirs.size() > 1)
            throw new ClusterInfoException(
                    "This is IMPOSSIBLE. There's more than one subdir of " + utils.leaderDir + ". They include " + imItSubdirs);

        // make sure it's still mine.
        @SuppressWarnings("unchecked")
        final C registered = (C) session.getData(utils.masterDetermineDir, null);

        return utils.thisNodeAddress.equals(registered); // am I it, or not?
    }
}
