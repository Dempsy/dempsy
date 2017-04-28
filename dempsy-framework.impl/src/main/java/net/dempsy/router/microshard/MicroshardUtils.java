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

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure.RootPaths;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.util.SafeString;

public class MicroshardUtils {
    private static Logger LOGGER = LoggerFactory.getLogger(MicroshardUtils.class);

    /**
    * The {@code clusterDir} is a subdirectory of the {@code application} directory. 
    * It's named by the clusterName from the given {@link ClusterId} and it contains
    * an instance of a {@link MicroShardClusterInformation}. It also serves as the 
    * root directory for the cluster's nodes and shards subdirectories.
    */
    public final String clusterDir;

    /**
     * The shardsDir is a subdirectory of the {@code clusterDir}. This is the directory 
     * where shards are managed across the distributed cluster.
     */
    public final String shardsDir;

    /**
    * A subdirectory of the @{code clusterDir}, the {@code nodesDir} contains an ephemeral and
    * sequential entry (subdirectory) per currently running node. Each of these subdirectories
    * contains an instance of a {@link DefaultShardInfo} which the manager will use to copy into 
    * the appropriate shardsDir subdirectory in order to accomplish an assignment.
    */
    public final String clusterNodesDir;

    /**
    * A subdirectory of the @{code clusterDir}, the {@code shardTxDirectory} is used to manage
    * "transactions" on the shards. This is to help prevent the "herd effect" on the zookeeper
    * server and have repeated updates for every shard that's grabbed. 
    */
    public final String shardTxDirectory;

    public final ClusterId clusterId;

    public final ClusterInfo clusterInfo;

    public MicroshardUtils(final RootPaths paths, final ClusterId clusterId, final ClusterInfo clusterInfo) {
        this.clusterDir = paths.clustersDir + "/" + clusterId.clusterName;
        this.shardsDir = clusterDir + "/shards";
        this.clusterNodesDir = clusterDir + "/nodes";
        this.shardTxDirectory = clusterDir + "/shardTxs";
        this.clusterId = clusterId;
        this.clusterInfo = clusterInfo;
    }

    public final void mkClusterDir(final ClusterInfoSession session) throws ClusterInfoException {
        session.recursiveMkdir(clusterDir, clusterInfo, DirMode.PERSISTENT, DirMode.PERSISTENT);
    }

    public final void mkAllDirs(final ClusterInfoSession session) throws ClusterInfoException {
        mkClusterDir(session);
        session.mkdir(shardsDir, null, DirMode.PERSISTENT);
        session.mkdir(clusterNodesDir, null, DirMode.PERSISTENT);
        session.mkdir(shardTxDirectory, null, DirMode.PERSISTENT);
    }

    /**
    * This will get the children of one of the main persistent directories (provided in the path parameter). If
    * the directory doesn't exist it will create all of the persistent directories, but only if the clusterInfo isn't null.
    */
    Collection<String> persistentGetMainDirSubdirs(final ClusterInfoSession session, final String path, final ClusterInfoWatcher watcher)
            throws ClusterInfoException {

        Collection<String> shardsFromClusterManager;
        try {
            shardsFromClusterManager = session.getSubdirs(path, watcher);
        } catch (final ClusterInfoException.NoNodeException e) {
            mkAllDirs(session);
            shardsFromClusterManager = session.getSubdirs(path, watcher);
        }
        return shardsFromClusterManager;
    }

    /**
    * Fill the map of shards to shardinfos for internal use. 
    * 
    * @param mapToFill is the map to fill from the ClusterInfo.
    * @param session is the ClusterInfoSession to get retrieve the data from.
    * @param watcher, if not null, will be set as the watcher on the shard directory for this cluster.
    * @return There's 3 possible return values:
    * <ul>
    * <li>-1. This happens when we're on the client side and we beat all registrations to checking.</li>
    * <li>0. This happens when we're on the {@link Inbound} side but we're the first in.</li> 
    * <li>The totalAddressCount from each shard. These are supposed to be repeated in each 
    * {@link ShardInfo}.</li>
    * </ul>
    */
    int fillMapFromActiveShards(final Map<Integer, ShardInfo> mapToFill, final ClusterInfoSession session, final ClusterInfoWatcher watcher)
            throws ClusterInfoException {
        int totalAddressCounts = -1;

        // First get the shards that are in transition.
        final Collection<String> shardsFromClusterManager = persistentGetMainDirSubdirs(session, shardsDir, watcher);

        if (shardsFromClusterManager != null) {
            // zero is valid but we only want to set it if we are not
            // going to enter into the loop below.
            if (shardsFromClusterManager.size() == 0)
                totalAddressCounts = 0;

            for (final String shard : shardsFromClusterManager) {
                final ShardInfo shardInfo = (ShardInfo) session.getData(shardsDir + "/" + shard, null);
                if (shardInfo != null) {
                    mapToFill.put(shardInfo.shardIndex, shardInfo);
                    if (totalAddressCounts == -1)
                        totalAddressCounts = shardInfo.totalAddress;
                    else if (totalAddressCounts != shardInfo.totalAddress)
                        LOGGER.error("There is a problem with the shards taken by the cluster manager for the cluster " +
                                clusterId + ". Shard " + shardInfo.shardIndex +
                                " from " + SafeString.objectDescription(shardInfo.destination) +
                                " thinks the total number of shards for this cluster is " + shardInfo.totalAddress +
                                " but a former shard said the total was " + totalAddressCounts);
                } else {
                    final String message = "Retrieved empty shard for cluster " + clusterId + ", shard number " + shard
                            + ". This should not be possible since the directory creating and the setting of the data are supposed to happen atomically.";
                    LOGGER.error(message);
                    throw new ClusterInfoException(message);
                }
            }
        }
        return totalAddressCounts;
    }

    public static class ClusterInfo {
        public final int minNodeCount;
        public final int totalShardCount;

        // required for deserialize
        @SuppressWarnings("unused")
        private ClusterInfo() {
            minNodeCount = 5;
            totalShardCount = 300;
        }

        public ClusterInfo(final int totalShardCount, final int minNodeCount) {
            this.totalShardCount = totalShardCount;
            this.minNodeCount = minNodeCount;
        }

        @Override
        public String toString() {
            return "{ minNodeCount:" + minNodeCount + ", totalShardCount:" + totalShardCount + "}";
        }
    }

    public static class ShardInfo {
        public final int totalAddress;
        public final int shardIndex;
        public final ContainerAddress destination;

        public ShardInfo(final ContainerAddress destination, final int shardNum, final int totalAddress) {
            this.destination = destination;
            this.shardIndex = shardNum;
            this.totalAddress = totalAddress;
        }

        @SuppressWarnings("unused")
        private ShardInfo() {
            totalAddress = -1;
            shardIndex = -1;
            destination = null;
        } // needed for JSON serialization

        @Override
        public String toString() {
            return "{ shardIndex:" + shardIndex + ", totalAddress:" + totalAddress + ", destination:" +
                    SafeString.objectDescription(destination) + "}";
        }
    }

}
