/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.router.microshard;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.microshard.MicroshardUtils.ClusterInfo;
import net.dempsy.router.microshard.MicroshardUtils.ShardInfo;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;
import net.dempsy.utils.PersistentTask;

/**
 * This Routing Strategy uses the collaborator to negotiate for control over a 
 * set of "micro shards" with other instances in the cluster.
 */
public class MicroshardingInbound implements RoutingStrategy.Inbound {

    private static final long RETRY_TIMEOUT = 500L;
    private static Logger LOGGER = LoggerFactory.getLogger(MicroshardingInbound.class);

    public static final String CONFIG_KEY_TOTAL_SHARDS = "total_shards";
    public static final String CONFIG_KEY_MIN_NODES = "min_node_count";
    public static final String DEFAULT_TOTAL_SHARDS = "256";
    public static final String DEFAULT_MIN_NODES = "1";

    protected int totalNumShards = Integer.parseInt(DEFAULT_TOTAL_SHARDS);
    protected int mask;
    protected int minNumberOfNodes = Integer.parseInt(DEFAULT_MIN_NODES);

    private AutoDisposeSingleThreadScheduler dscheduler;

    // destinationsAcquired should only be modified through the modifyDestinationsAcquired method.
    private final Set<Integer> destinationsAcquired = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    private ClusterInfoSession session;
    private ContainerAddress thisNodeAddress;
    private ClusterId clusterId;
    private KeyspaceChangeListener listener = null;
    private MicroshardUtils msutils;
    private ClusterInfo clusterInfo;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private PersistentTask shardChangeWatcher;
    private Transaction tx;

    @Override
    public void start(final Infrastructure infra) {
        this.dscheduler = infra.getScheduler();
        this.session = infra.getCollaborator();
        this.clusterInfo = new ClusterInfo(totalNumShards, minNumberOfNodes);
        this.msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, clusterInfo);

        totalNumShards = Integer
                .parseInt(infra.getConfigValue(MicroshardingInbound.class, CONFIG_KEY_TOTAL_SHARDS, DEFAULT_TOTAL_SHARDS));

        if (Integer.bitCount(totalNumShards) != 1)
            throw new IllegalArgumentException("The configuration property \"" + CONFIG_KEY_TOTAL_SHARDS
                    + "\" must be set to a power of 2. It's currently set to " + totalNumShards);

        mask = totalNumShards - 1;
        minNumberOfNodes = Integer.parseInt(infra.getConfigValue(MicroshardingInbound.class, CONFIG_KEY_MIN_NODES, DEFAULT_MIN_NODES));

        this.shardChangeWatcher = getShardChangeWatcher();
        tx = new Transaction(msutils.shardTxDirectory, session, shardChangeWatcher);
        shardChangeWatcher.process(); // this invokes the acquireShards logic
    }

    @Override
    public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener listener) {
        this.thisNodeAddress = address;
        this.clusterId = clusterId;
        this.listener = listener;
    }

    private void modifyDestinationsAcquired(final Collection<Integer> toRemove, final Collection<Integer> toAdd) {
        if (LOGGER.isTraceEnabled())
            LOGGER.trace(toString() + " reconfiguring with toAdd:" + toAdd + ", toRemove:" + toRemove);
        if (toRemove != null)
            destinationsAcquired.removeAll(toRemove);
        if (toAdd != null)
            destinationsAcquired.addAll(toAdd);
        if (LOGGER.isTraceEnabled())
            LOGGER.trace(toString() + "<- now looks like");
    }

    @Override
    public String toString() {
        return MicroshardingInbound.class.getSimpleName() + " at " + thisNodeAddress + " for " + clusterId + " owning " + destinationsAcquired;
    }

    private String nodeDirectory = null;

    // ==============================================================================
    // This PersistentTask watches the shards directory for changes and will make
    // make sure that the
    // ==============================================================================
    private PersistentTask getShardChangeWatcher() {
        return new PersistentTask(LOGGER, isRunning, dscheduler, RETRY_TIMEOUT) {
            final boolean traceOn = LOGGER.isTraceEnabled();

            @Override
            public String toString() {
                return "determin and participate in shard distribution for " + clusterId + " from " + thisNodeAddress.node;
            }

            private final int checkNodeDirectory() throws ClusterInfoException {
                try {

                    // get all of the nodes in the nodeDir and set one for this node if it's not already set.
                    final Collection<String> nodeDirs;
                    if (nodeDirectory == null) {
                        nodeDirectory = session.recursiveMkdir(msutils.clusterNodesDir + "/node_", thisNodeAddress, DirMode.PERSISTENT,
                                DirMode.EPHEMERAL_SEQUENTIAL);
                        nodeDirs = session.getSubdirs(msutils.clusterNodesDir, this);
                    } else
                        nodeDirs = msutils.persistentGetMainDirSubdirs(session, msutils.clusterNodesDir, this);

                    // what node am I?
                    int nodeRank = -1;
                    int index = 0;
                    final String nodeSubdir = new File(nodeDirectory).getName();
                    for (final String cur : nodeDirs) {
                        if (nodeSubdir.equals(cur)) {
                            nodeRank = index;
                            break;
                        }
                        index++;
                    }

                    // If I couldn't find me, there's a problem.
                    if (nodeRank == -1) {
                        // drop the node directory since it clearly doesn't exist anymore
                        nodeDirectory = null;
                        throw new ClusterInfoException(
                                "Node " + thisNodeAddress + " was registered at " + nodeSubdir + " but it wasn't found as a subdirectory.");
                    }

                    // verify the container address is correct.
                    ContainerAddress curDest = (ContainerAddress) session.getData(nodeDirectory, null);
                    if (curDest == null)
                        session.setData(nodeDirectory, thisNodeAddress);
                    else if (!thisNodeAddress.equals(curDest)) { // wth?
                        final String tmp = nodeDirectory;
                        nodeDirectory = null;
                        throw new ClusterInfoException("Impossible! The Node directory " + tmp + " contains the destination for " + curDest
                                + " but should have " + thisNodeAddress);
                    }

                    // Check to make sure that THIS node is only in one place. If we find it in a node directory
                    // that's not THIS node directory then this is something we should clean up.
                    for (final String subdir : nodeDirs) {
                        final String fullPathToSubdir = msutils.clusterNodesDir + "/" + subdir;
                        curDest = (ContainerAddress) session.getData(fullPathToSubdir, null);
                        if (thisNodeAddress.equals(curDest) && !fullPathToSubdir.equals(nodeDirectory)) // this is bad .. clean up
                            session.rmdir(fullPathToSubdir);
                    }

                    return nodeRank;
                } catch (final ClusterInfoException cie) {
                    cleanupAfterExceptionDuringNodeDirCheck();
                    throw cie;
                } catch (final RuntimeException re) {
                    cleanupAfterExceptionDuringNodeDirCheck();
                    throw re;
                } catch (final Throwable th) {
                    cleanupAfterExceptionDuringNodeDirCheck();
                    throw new RuntimeException("Unknown exception!", th);
                }
            }

            // called from the catch clauses in checkNodeDirectory
            private final void cleanupAfterExceptionDuringNodeDirCheck() {
                if (nodeDirectory != null) {
                    // attempt to remove the node directory
                    try {
                        if (session.exists(nodeDirectory, this)) {
                            session.rmdir(nodeDirectory);
                        }
                        nodeDirectory = null;
                    } catch (final ClusterInfoException cie2) {}
                }
            }

            @Override
            public boolean execute() {
                if (traceOn)
                    LOGGER.trace(MicroshardingInbound.this.toString() + " Resetting Inbound Strategy.");

                final Random random = new Random();

                try {
                    tx.mkRootDirs();
                    tx.watch(); // need to watch the transaciton dir.

                    // check node directory
                    final int nodeRank = checkNodeDirectory();

                    final int currentWorkingNodeCount = findWorkingNodeCount(session, msutils, minNumberOfNodes, null);
                    final int numberIShouldHave = howManyShouldIHave(totalNumShards, currentWorkingNodeCount, nodeRank);

                    // we are rebalancing the shards so we will figure out what we are removing
                    // and adding.
                    final Set<Integer> destinationsToRemove = new HashSet<Integer>();
                    final Set<Integer> destinationsToAdd = new HashSet<Integer>();

                    // ==============================================================================
                    // need to verify that the existing shards in destinationsAcquired are still ours.
                    final Map<Integer, ShardInfo> shardNumbersToShards = new HashMap<Integer, ShardInfo>();
                    msutils.fillMapFromActiveShards(shardNumbersToShards, session, null);

                    // First, are there any I don't know about that are in shardNumbersToShards.
                    // This could be because I was assigned a shard (or in a previous execute, I acquired one
                    // but failed prior to accounting for it). In this case there will be shards in
                    // shardNumbersToShards that are assigned to me but aren't in destinationsAcquired.
                    //
                    // We are also piggy-backing off this loop to count the numberOfShardsWeActuallyHave
                    int numberOfShardsWeActuallyHave = 0;
                    for (final Map.Entry<Integer, ShardInfo> entry : shardNumbersToShards.entrySet()) {
                        if (thisNodeAddress.equals(entry.getValue().destination)) {
                            numberOfShardsWeActuallyHave++; // this entry is a shard that's assigned to us
                            final Integer shardNumber = entry.getKey();
                            if (!destinationsAcquired.contains(shardNumber)) // if we never saw it then we need to take it.
                                destinationsToAdd.add(shardNumber);
                        }
                    }

                    if (traceOn)
                        LOGGER.trace(
                                "" + MicroshardingInbound.this + " has these destinations that I didn't know about " + destinationsToAdd);

                    // Now we are going to go through what we think we have and see if any are missing.
                    final Collection<Integer> shardsToReaquire = new ArrayList<Integer>();
                    for (final Integer destinationShard : destinationsAcquired) {
                        // select the corresponding shard information
                        final ShardInfo shardInfo = shardNumbersToShards.get(destinationShard);
                        if (shardInfo == null || !thisNodeAddress.equals(shardInfo.destination))
                            shardsToReaquire.add(destinationShard);
                    }

                    if (traceOn)
                        LOGGER.trace(
                                MicroshardingInbound.this.toString() + " has these destiantions that it seemed to have lost "
                                        + shardsToReaquire);
                    // ==============================================================================

                    // ==============================================================================
                    // Now re-acquire the potentially lost shards.
                    for (final Integer shardToReaquire : shardsToReaquire) {
                        // if we already have too many shards then there's no point in trying
                        // to reacquire the the shard.
                        if (numberOfShardsWeActuallyHave >= numberIShouldHave) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " removing " + shardToReaquire +
                                        " from one's I care about because I already have " + numberOfShardsWeActuallyHave +
                                        " but only need " + numberIShouldHave);

                            destinationsToRemove.add(shardToReaquire); // we're going to skip it ... and drop it.
                        }
                        // otherwise we will try to reacquire it.
                        else if (!acquireShard(shardToReaquire, shardNumbersToShards)) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " removing " + shardToReaquire +
                                        " from one's I care about because I couldn't reaquire it.");

                            LOGGER.info("Cannot reaquire the shard " + shardToReaquire + " for the cluster " + clusterId);
                            // I need to drop the shard from my list of destinations
                            destinationsToRemove.add(shardToReaquire);
                        } else { // otherwise, we successfully reacquired it.
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " reacquired " + shardToReaquire);

                            numberOfShardsWeActuallyHave++; // we have one more.
                        }
                    }
                    // ==============================================================================

                    // ==============================================================================
                    // Here, if we have too many shards, we will give up anything in the list destinationsToAdd
                    // until we are either at the level were we should be, or we have no more to give up
                    // from the list of those we were planning on adding.
                    Iterator<Integer> curPos = destinationsToAdd.iterator();
                    while (numberOfShardsWeActuallyHave > numberIShouldHave &&
                            destinationsToAdd.size() > 0 && curPos.hasNext()) {
                        final Integer cur = curPos.next();
                        if (doIOwnThisShard(cur, shardNumbersToShards)) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " removing shard " + cur + " because I already have " +
                                        numberOfShardsWeActuallyHave + " but can give up some until I have " + numberIShouldHave
                                        + " and was planning on adding it.");

                            curPos.remove();
                            session.rmdir(msutils.shardsDir + "/" + cur);
                            tx.open();
                            numberOfShardsWeActuallyHave--;
                            shardNumbersToShards.remove(cur);
                        }
                    }
                    // ==============================================================================

                    // ==============================================================================
                    // Here, if we still have too many shards, we will begin deleting destinationsToRemove
                    // that we may own actually own.
                    curPos = destinationsToRemove.iterator();
                    while (numberOfShardsWeActuallyHave > numberIShouldHave &&
                            destinationsToRemove.size() > 0 && curPos.hasNext()) {
                        final Integer cur = curPos.next();
                        if (doIOwnThisShard(cur, shardNumbersToShards)) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " removing shard " + cur + " because I already have " +
                                        numberOfShardsWeActuallyHave + " but can give up some until I have " + numberIShouldHave +
                                        " and was planning on removing it anyway.");

                            session.rmdir(msutils.shardsDir + "/" + cur);
                            tx.open();
                            numberOfShardsWeActuallyHave--;
                            shardNumbersToShards.remove(cur);
                        }
                    }
                    // ==============================================================================

                    // ==============================================================================
                    // above we bled off the destinationsToAdd. Now we remove actually known destinationsAcquired
                    final Iterator<Integer> destinationsAcquiredIter = destinationsAcquired.iterator();
                    while (numberOfShardsWeActuallyHave > numberIShouldHave && destinationsAcquiredIter.hasNext()) {
                        final Integer cur = destinationsAcquiredIter.next();
                        // if we're already set to remove it because it didn't appear in the initial fillMapFromActiveShards
                        // then there's no need to remove it from the session as it's already gone.
                        if (doIOwnThisShard(cur, shardNumbersToShards)) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " removing shard " + cur + " because I already have " +
                                        numberOfShardsWeActuallyHave + " but can give up some until I have " + numberIShouldHave + ".");

                            session.rmdir(msutils.shardsDir + "/" + cur);
                            tx.open();
                            numberOfShardsWeActuallyHave--;
                            destinationsToRemove.add(cur);
                            shardNumbersToShards.remove(cur);
                        }
                    }
                    // ==============================================================================

                    // ==============================================================================
                    // Now see if we need to grab more shards. Maybe we just came off backup or, in
                    // the case of elasticity, maybe another node went down.
                    if (traceOn && numberOfShardsWeActuallyHave < numberIShouldHave)
                        LOGGER.trace(MicroshardingInbound.this.toString() + " considering grabbing more shards given that I have "
                                + numberOfShardsWeActuallyHave
                                + " but could have a max of " + numberIShouldHave);

                    List<Integer> shardsToTry = null;
                    Collection<String> subdirs;
                    while (((subdirs = session.getSubdirs(msutils.shardsDir, null)).size() < totalNumShards) &&
                            (numberOfShardsWeActuallyHave < numberIShouldHave)) {
                        if (traceOn)
                            LOGGER.trace(MicroshardingInbound.this.toString() + " will try to grab more shards.");

                        if (shardsToTry == null || shardsToTry.size() == 0)
                            shardsToTry = range(subdirs, totalNumShards);
                        if (shardsToTry.size() == 0) {
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " all other shards are taken.");
                            break;
                        }
                        final int randomIndex = random.nextInt(shardsToTry.size());
                        final int shardToTry = shardsToTry.remove(randomIndex);

                        // if we're already considering this shard ...
                        if (!doIOwnThisShard(shardToTry, shardNumbersToShards) &&
                                acquireShard(shardToTry, shardNumbersToShards)) {
                            if (!destinationsAcquired.contains(shardToTry))
                                destinationsToAdd.add(shardToTry);
                            numberOfShardsWeActuallyHave++;
                            if (traceOn)
                                LOGGER.trace(MicroshardingInbound.this.toString() + " got a new shard " + shardToTry);

                        }
                    }
                    // ==============================================================================

                    if (destinationsToRemove.size() > 0 || destinationsToAdd.size() > 0) {
                        final String previous = MicroshardingInbound.this.toString();
                        modifyDestinationsAcquired(destinationsToRemove, destinationsToAdd);

                        if (traceOn)
                            LOGGER.trace(
                                    previous + " keyspace notification (" + (destinationsToRemove.size() > 0) + "," + (destinationsToAdd.size() > 0)
                                            + ")");
                        listener.keyspaceChanged(destinationsToRemove.size() > 0, destinationsToAdd.size() > 0, MicroshardingInbound.this);
                    }

                    if (LOGGER.isTraceEnabled())
                        LOGGER.trace(MicroshardingInbound.this.toString() + " Succesfully reset Inbound Strategy for cluster " + clusterId);

                    tx.close();
                    return true;
                } catch (final ClusterInfoException cie) {
                    throw new RuntimeException(cie); // let them know we failed
                }
            }

        };
    }
    // ==============================================================================

    @Override
    public boolean isReady() {
        // we are going to assume we're initialized when all of the shards are accounted for.
        // We want to go straight at the cluster info since destinationsAcquired may be out
        // of date in the case where the cluster manager is down.
        try {
            if (nodeDirectory == null)
                return false;

            if (!thisNodeAddress.equals(session.getData(nodeDirectory, null)))
                return false;

            if (session.getSubdirs(msutils.shardsDir, null).size() != totalNumShards)
                return false;

            // make sure we have all of the nodes we should have
            final int numNodes = session.getSubdirs(msutils.clusterNodesDir, null).size();

            final boolean ret = (destinationsAcquired.size() >= (int) Math.floor((double) totalNumShards / (double) numNodes)) &&
                    (destinationsAcquired.size() <= (int) Math.ceil((double) totalNumShards / (double) numNodes));
            return ret;
        } catch (final ClusterInfoException e) {
            return false;
        } catch (final RuntimeException re) {
            LOGGER.debug("", re);
            return false;
        }
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

    // we multiply by an arbitrary large (not 31) prime in order to
    // avoid overlapping collisions with the normal HashMap operation
    // used in the container implementations.
    final static int prime = 514229;

    @Override
    public boolean doesMessageKeyBelongToNode(final Object messageKey) {
        // normally this would need to be passed to Math.abs however, using a 'mask' rather
        // than a mod means this number can never be negative.
        return destinationsAcquired.contains((prime * messageKey.hashCode()) & mask);
    }

    public int getNumShardsCovered() {
        return destinationsAcquired.size();
    }

    private final boolean doIOwnThisShard(final Integer shard, final Map<Integer, ShardInfo> shardNumbersToShards) {
        final ShardInfo si = shardNumbersToShards.get(shard);
        if (si == null)
            return false;
        return thisNodeAddress.equals(si.destination);
    }

    private final int findWorkingNodeCount(final ClusterInfoSession session, final MicroshardUtils msutils, final int minNodeCount,
            final ClusterInfoWatcher nodeDirectoryWatcher) throws ClusterInfoException {
        final Collection<String> nodeDirs = session.getSubdirs(msutils.clusterNodesDir, nodeDirectoryWatcher);
        if (LOGGER.isTraceEnabled())
            LOGGER.trace(toString() + " Fetching all node's subdirectories:" + nodeDirs);

        // We CANNOT only consider node directories that have a Destination because we are not listening for them.
        // The only way to require the destination is to fail here if any destinations are null and retry later
        // in an attempt to wait until whatever node just created this directory gets around to setting the
        // Destination. For now we will assume if the directory exists, then the node exists.
        final int curRegisteredNodesCount = nodeDirs.size();
        return curRegisteredNodesCount < minNodeCount ? minNodeCount : curRegisteredNodesCount;
    }

    private boolean acquireShard(final int shardNum, final Map<Integer, ShardInfo> shardNumbersToShards) throws ClusterInfoException {
        final ShardInfo dest = new ShardInfo(thisNodeAddress, shardNum, totalNumShards);
        final String shardPath = msutils.shardsDir + "/" + String.valueOf(shardNum);
        if (session.mkdir(shardPath, dest, DirMode.EPHEMERAL) != null) {
            tx.open();
            if (shardNumbersToShards != null)
                shardNumbersToShards.put(shardNum, dest);

            return true;
        } else
            return false;
    }

    private final static List<Integer> range(final Collection<String> curSubdirs, final int max) {
        final List<Integer> ret = new ArrayList<Integer>(max);
        for (int i = 0; i < max; i++) {
            if (!curSubdirs.contains(Integer.toString(i)))
                ret.add(i);
        }
        return ret;
    }

    private final static int howManyShouldIHave(final int totalShardCount, final int numNodes, final int myRank) {
        final int base = Math.floorDiv(totalShardCount, numNodes);
        final int mod = Math.floorMod(totalShardCount, numNodes);
        return myRank < mod ? (base + 1) : base;
    }
}