package net.dempsy.router.managed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.dempsy.Infrastructure;
import net.dempsy.Infrastructure.RootPaths;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoException.NoNodeException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.microshard.MicroshardingInbound;

public class Utils {
    // we multiply by an arbitrary large (not 31) prime in order to
    // avoid overlapping collisions with the normal HashMap operation
    // used in the container implementations.
    final static int prime = 514229;

    public static final String CONFIG_KEY_TOTAL_SHARDS = "total_shards";
    public static final String CONFIG_KEY_MIN_NODES = "min_node_count";
    public static final String DEFAULT_TOTAL_SHARDS = "256";
    public static final String DEFAULT_MIN_NODES = "1";

    /**
     * PERSISTENT directory at: {@code /[appname]/clusters/[clustername]}
     */
    public final String clusterDir;

    /**
     * PERSISTENT directory at: {@code /[appname]/clusters/[clustername]/leader}
     */
    public final String leaderDir;

    /**
     * <p>EPHEMERAL directory at: {@code /[appname]/clusters/[clustername]/managed/ImIt}</p>
     * 
     * <p>This directory is the location for determining who is the master (who is "it"). </p>
     */
    public final String masterDetermineDir;

    /**
     * PERSISTENT directory at: {@code /[appname]/clusters/[clustername]/shardAssignment}
     * 
     * The object here contains the shard assignements.
     */
    public final String shardsAssignedDir;

    /**
     * PERSISTENT directory at: {@code /[appname]/clusters/[clustername]/nodes}
     * 
     * Subdirectories here are {@link DirMode.EPHEMERAL_SEQUENTIAL} and contain data for each node currently 
     * participating.
     */
    public final String nodesDir;

    public final int totalNumShards;
    public final int mask;
    public final int minNumberOfNodes;

    public final ContainerAddress thisNodeAddress;

    public final ClusterId clusterId;
    public final ClusterInfoSession session;

    public Utils(final Infrastructure infra, final ClusterId clusterId, final ContainerAddress thisNode) {
        final RootPaths paths = infra.getRootPaths();

        this.clusterDir = paths.clustersDir + "/" + clusterId.clusterName;
        this.leaderDir = this.clusterDir + "/leader";
        this.masterDetermineDir = this.leaderDir + "/ImIt";
        this.shardsAssignedDir = this.clusterDir + "/shardAssignment";
        this.nodesDir = this.clusterDir + "/nodes";

        this.session = infra.getCollaborator();

        this.thisNodeAddress = thisNode;

        this.clusterId = clusterId;

        totalNumShards = Integer
                .parseInt(infra.getConfigValue(MicroshardingInbound.class, CONFIG_KEY_TOTAL_SHARDS, DEFAULT_TOTAL_SHARDS));

        if (Integer.bitCount(totalNumShards) != 1)
            throw new IllegalArgumentException("The configuration property \"" + CONFIG_KEY_TOTAL_SHARDS
                    + "\" must be set to a power of 2. It's currently set to " + totalNumShards);

        mask = totalNumShards - 1;
        minNumberOfNodes = Integer.parseInt(infra.getConfigValue(MicroshardingInbound.class, CONFIG_KEY_MIN_NODES, DEFAULT_MIN_NODES));
    }

    public Collection<String> persistentGetSubdir(final String path, final ClusterInfoWatcher watcher)
            throws ClusterInfoException {
        // first just see if we can get it.
        try {
            return session.getSubdirs(path, watcher);
        } catch (final NoNodeException nne) {
            // okay, create me.
            session.recursiveMkdir(path, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
            return session.getSubdirs(path, watcher);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> persistentGetSubdirData(final String path, final ClusterInfoWatcher dirWatcher, final ClusterInfoWatcher dataWatcher)
            throws ClusterInfoException {
        final Collection<String> subdirs = persistentGetSubdir(path, dirWatcher);

        final ArrayList<T> ret = subdirs == null ? new ArrayList<>() : new ArrayList<>(subdirs.size());
        if (subdirs != null) {
            for (final String subdir : subdirs) {
                ret.add((T) session.getData(path + "/" + subdir, dataWatcher));
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <T> T persistentGetData(final String path, final ClusterInfoWatcher dataWatcher)
            throws ClusterInfoException {
        try {
            return (T) session.getData(path, dataWatcher);
        } catch (final NoNodeException nne) {
            session.recursiveMkdir(path, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
            if (dataWatcher != null)
                return (T) session.getData(path, dataWatcher);
            return null;
        }
    }

    public static class SubdirAndData<T> {
        public final String subdir;
        public final T data;

        public SubdirAndData(final String subdir, final T data) {
            this.subdir = subdir;
            this.data = data;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> List<SubdirAndData<T>> persistentGetSubdirAndData(final String path, final ClusterInfoWatcher dirWatcher,
            final ClusterInfoWatcher dataWatcher) throws ClusterInfoException {
        final Collection<String> subdirs = persistentGetSubdir(path, dirWatcher);

        final ArrayList<SubdirAndData<T>> ret = subdirs == null ? new ArrayList<>() : new ArrayList<>(subdirs.size());
        if (subdirs != null) {
            for (final String subdir : subdirs) {
                ret.add(new SubdirAndData<T>(subdir, (T) session.getData(path + "/" + subdir, dataWatcher)));
            }
        }
        return ret;
    }

    public int determineShard(final Object key) {
        return (prime * key.hashCode()) & mask;
    }

    public static <T> void rankSort(final List<SubdirAndData<T>> toSort) {
        Collections.sort(toSort, (o1, o2) -> o1.subdir.compareTo(o2.subdir));
    }

    public static class ShardAssignment implements Serializable {
        private static final long serialVersionUID = 1L;
        public final int[] shards;
        public final ContainerAddress addr;

        public ShardAssignment(final int[] shards, final ContainerAddress addr) {
            this.shards = shards;
            this.addr = addr;
        }

        @SuppressWarnings("unused") // serialization
        private ShardAssignment() {
            this(null, null);
        }
    }

}
