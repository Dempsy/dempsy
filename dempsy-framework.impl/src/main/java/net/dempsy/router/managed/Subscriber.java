package net.dempsy.router.managed;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.managed.Utils.ShardAssignment;
import net.dempsy.utils.PersistentTask;

public class Subscriber extends PersistentTask {
    private static Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

    private final Utils utils;
    private final ContainerAddress thisNode;
    private final AtomicReference<boolean[]> iOwn = new AtomicReference<boolean[]>(null);
    private final KeyspaceChangeListener listener;
    private String nodeDirectory = null;
    private final ClusterInfoSession session;

    public Subscriber(final Utils msutils, final Infrastructure infra, final AtomicBoolean isRunning, final KeyspaceChangeListener listener) {
        super(LOGGER, isRunning, infra.getScheduler(), 500);
        this.utils = msutils;
        this.session = msutils.session;
        this.thisNode = utils.thisNodeAddress;
        this.listener = listener;
    }

    public boolean isReady() {
        return iOwn.get() != null;
    }

    @Override
    public boolean execute() {
        try {
            checkNodeDirectory();
            final List<ShardAssignment> assignments = utils.persistentGetData(utils.shardsAssignedDir, this);
            if (assignments == null)
                return false; // nothing there, try later.

            for (final ShardAssignment sa : assignments) {
                // find me.
                if (thisNode.equals(sa.addr)) { // found me
                    final boolean[] newState = new boolean[utils.totalNumShards];
                    for (final int index : sa.shards)
                        newState[index] = true;
                    final boolean[] prev = Optional.ofNullable(iOwn.getAndSet(newState)).orElse(new boolean[utils.totalNumShards]);
                    callListener(prev, newState);
                    return true;
                }
            }

            return false;
        } catch (final ClusterInfoException cie) {
            throw new RuntimeException(cie);
        }
    }

    public boolean doIOwnShard(final int shard) throws IllegalStateException {
        final boolean[] ownership = iOwn.get();
        if (ownership == null)
            throw new IllegalStateException("Inbound for " + thisNode);
        return ownership[shard];
    }

    // ===============================================================
    // Test methods
    // ===============================================================

    int numShardsIOwn() {
        final boolean[] shards = iOwn.get();
        if (shards == null)
            return 0;
        return (int) IntStream.range(0, shards.length).mapToObj(i -> Boolean.valueOf(shards[i])).filter(b -> b.booleanValue()).count();
    }

    Utils getUtils() {
        return utils;
    }

    // ===============================================================

    private final int checkNodeDirectory() throws ClusterInfoException {
        try {

            // get all of the nodes in the nodeDir and set one for this node if it's not already set.
            final Collection<String> nodeDirs;
            if (nodeDirectory == null) {
                nodeDirectory = utils.session.recursiveMkdir(utils.nodesDir + "/node_", thisNode, DirMode.PERSISTENT,
                        DirMode.EPHEMERAL_SEQUENTIAL);
                nodeDirs = session.getSubdirs(utils.nodesDir, this);
            } else
                nodeDirs = utils.persistentGetSubdir(utils.nodesDir, null);

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
                        "Node " + thisNode + " was registered at " + nodeSubdir + " but it wasn't found as a subdirectory.");
            }

            // verify the container address is correct.
            ContainerAddress curDest = (ContainerAddress) session.getData(nodeDirectory, null);
            if (curDest == null) // this really can't be null
                session.setData(nodeDirectory, thisNode);
            else if (!thisNode.equals(curDest)) { // wth?
                final String tmp = nodeDirectory;
                nodeDirectory = null;
                throw new ClusterInfoException("Impossible! The Node directory " + tmp + " contains the destination for " + curDest
                        + " but should have " + thisNode);
            }

            // Check to make sure that THIS node is only in one place. If we find it in a node directory
            // that's not THIS node directory then this is something we should clean up.
            for (final String subdir : nodeDirs) {
                final String fullPathToSubdir = utils.nodesDir + "/" + subdir;
                curDest = (ContainerAddress) session.getData(fullPathToSubdir, null);
                if (thisNode.equals(curDest) && !fullPathToSubdir.equals(nodeDirectory)) // this is bad .. clean up
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

    private void callListener(final boolean[] prev, final boolean[] next) {
        boolean grow = false;
        boolean shrink = false;
        for (int i = 0; i < prev.length && !(grow && shrink); i++) {
            if (!prev[i] && next[i])
                grow = true;
            if (prev[i] && !next[i])
                shrink = true;
        }

        try {
            listener.keyspaceChanged(shrink, grow);
        } catch (final RuntimeException rte) {
            LOGGER.error("Exception while notifying " + KeyspaceChangeListener.class.getSimpleName() + " of a change (" +
                    (grow && shrink ? "gained and lost some shards)" : (grow ? "gained some shards)" : "lost some shards)")), rte);
        }
    }
}
