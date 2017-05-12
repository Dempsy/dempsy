package net.dempsy;

import java.util.Map;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public interface Infrastructure {

    /**
     * The globally set {@link ClusterInfoSession} available to the entire application.
     * This instance is the one for the entire node.
     */
    ClusterInfoSession getCollaborator();

    /**
     * A node-centric {@link AutoDisposeSingleThreadScheduler}
     */
    AutoDisposeSingleThreadScheduler getScheduler();

    /**
     * The {@link ClusterInfoSession} paths for the application this node is part of. 
     */
    RootPaths getRootPaths();

    /**
     * The Infrastructure will supply a {@link ClusterStatsCollector} using the node's 
     * configured {@link ClusterStatsCollectorFactory} supplying the given {@link ClusterId}.
     */
    ClusterStatsCollector getClusterStatsCollector(ClusterId clusterId);

    /**
     * This method will retreive the single instance of a {@link NodeStatsCollector} for 
     * this node.
     */
    NodeStatsCollector getNodeStatsCollector();

    /**
     * User configuration for the node is accessible from this method. 
     */
    public default Map<String, String> getConfiguration() {
        return getNode().getConfiguration();
    }

    /**
     * A nodeId that should only be used for logging purposes.
     */
    String getNodeId();

    /**
     * The current {@link ThreadingModel} set for the node. This is usually used to start
     * daemon threads that might be necessary to run some {@link Service}s.
     */
    ThreadingModel getThreadingModel();

    /**
     * A helper method to retrieve a specific piece of configuration for the given 
     * class. 
     */
    public default String getConfigValue(final Class<?> clazz, final String key, final String defaultValue) {
        final Map<String, String> conf = getConfiguration();
        final String entireKey = clazz.getPackage().getName() + "." + key;
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    public Node getNode();

    /**
     * The {@link ClusterInfoSession} paths for the application this node is part of. 
     */
    public static class RootPaths {
        /**
         * Applications are name-space separated in zookeeper by the application name.
         * This is what makes up the rootDir.
         */
        public final String rootDir;

        /**
         * The nodes directory for an application is [rootDir]/nodes.
         */
        public final String nodesDir;

        /**
         * The clusterDir for an application is [rootDir]/clusters
         */
        public final String clustersDir;

        public RootPaths(final String rootDir, final String nodesDir, final String clustersDir) {
            this.rootDir = rootDir;
            this.nodesDir = nodesDir;
            this.clustersDir = clustersDir;
        }

        public RootPaths(final String appName) {
            this(root(appName), nodes(appName), clusters(appName));
        }

        private static String root(final String application) {
            return "/" + application;
        }

        private static String nodes(final String application) {
            return root(application) + "/nodes";
        }

        private static String clusters(final String application) {
            return root(application) + "/clusters";
        }
    }

}
