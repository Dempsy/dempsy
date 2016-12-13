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

package net.dempsy.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.Dempsy;
import net.dempsy.DempsyException;

public class RunNode {
    protected static Logger logger = LoggerFactory.getLogger(RunNode.class);
    protected static final String appdefParam = "appdef";
    protected static final String applicationParam = "application";
    protected static final String zk_connectParam = "zk_connect";
    protected static final String zk_timeoutParam = "zk_session_timeout";
    protected static final String zk_timeoutDefault = "5000";
    protected static final String total_slots_per_clusterParam = "total_slots_for_cluster";
    protected static final String total_slots_per_clusterDefault = "100";
    protected static final String min_num_nodes_per_clusterParam = "min_nodes_for_cluster";
    protected static final String min_num_nodes_per_clusterDefault = "3";

    public static void main(final String[] args) {
        try {
            run(args);
        } catch (final Throwable e) {
            e.printStackTrace(System.err);
            System.err.flush();
            System.exit(1);
        }
        System.exit(0);
    }

    public static void run(final String[] args) throws DempsyException {
        // ======================================================
        // Handle all of the options.

        final String application = System.getProperty(applicationParam);
        if (application == null || application.length() == 0)
            usage("the java vm option \"-D" + applicationParam + "\" wasn't specified.");

        final String[] clusterIdParts = application.split(":");
        if (clusterIdParts.length != 2)
            usage("invalid format for the -Dcluster option. It should be a clusterid of the form \"applicationName:clusterName.\"");

        System.setProperty("application", clusterIdParts[0]);
        System.setProperty("cluster", clusterIdParts[1]);

        String appCtxFilename = System.getProperty(appdefParam);
        if (appCtxFilename == null || appCtxFilename.length() == 0) {
            // usage("the java vm option \"-D" + appdefParam + "\" wasn't specified.");
            appCtxFilename = "DempsyApplicationContext-" + clusterIdParts[0] + ".xml";
        }

        final String zkConnect = System.getProperty(zk_connectParam);
        if (zkConnect == null || zkConnect.length() == 0)
            usage("the java vm option \"-D" + zk_connectParam + "\" wasn't specified.");

        final String zkTimeout = System.getProperty(zk_timeoutParam);
        if (zkTimeout == null || zkTimeout.length() == 0)
            System.setProperty(zk_timeoutParam, zk_timeoutDefault);

        final String totalSlots = System.getProperty(total_slots_per_clusterParam);
        if (totalSlots == null || totalSlots.length() == 0)
            System.setProperty(total_slots_per_clusterParam, total_slots_per_clusterDefault);

        final String minNodes = System.getProperty(min_num_nodes_per_clusterParam);
        if (minNodes == null || minNodes.length() == 0)
            System.setProperty(min_num_nodes_per_clusterParam, min_num_nodes_per_clusterDefault);
        // ======================================================

        final String contextFile = "classpath:Dempsy-distributed.xml";
        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] { appCtxFilename, contextFile })) {
            context.registerShutdownHook();
            try {
                context.getBean(Dempsy.class).waitToBeStopped();
            } catch (final InterruptedException e) {
                logger.error("Interrupted . . . ", e);
            }

            logger.info("Shut down dempsy appliction " + appCtxFilename + "-" + application + ", bye!");
        }
    }

    public static void usage(final String errorMessage) throws DempsyException {
        final StringBuilder sb = new StringBuilder();
        if (errorMessage != null)
            sb.append("ERROR:" + errorMessage + "\n");
        sb.append("usage example: java -D" + appdefParam + "=MyAppDefinition.xml -D" + applicationParam +
                "=ClusterToStart -D" + zk_connectParam + "=ZookeeperConnectUrlString [-D" + zk_timeoutParam +
                "=" + zk_timeoutDefault + "] [-D" + total_slots_per_clusterParam + "=" + total_slots_per_clusterDefault +
                "] [-D" + min_num_nodes_per_clusterParam + "=" + min_num_nodes_per_clusterDefault +
                "] -cp (classpath) " + RunNode.class.getName() + "\n");
        sb.append(
                "    -D" + appdefParam + " must be supplied to indicate the Dempsy application definition's spring application context xml file.\n");
        sb.append("           A file with the name given that contains a spring application context xml must be on the classpath.\n");
        sb.append("    -D" + applicationParam
                + " should fully specify which cluster we are starting using the clusterid format \"appname:clustername\"\n");
        sb.append("    -D" + zk_connectParam + " should specify the connect string for zookeeper.\n");
        sb.append(
                "           see: http://zookeeper.apache.org/doc/r3.2.2/api/org/apache/zookeeper/ZooKeeper.html#ZooKeeper%28java.lang.String,%20int,%20org.apache.zookeeper.Watcher%29\n");
        sb.append("    -D" + zk_timeoutParam + " is optional. It should specify the session timeout for zookeeper. The default is "
                + zk_timeoutDefault + "\n");
        sb.append(
                "           see: http://zookeeper.apache.org/doc/r3.2.2/api/org/apache/zookeeper/ZooKeeper.html#ZooKeeper%28java.lang.String,%20int,%20org.apache.zookeeper.Watcher%29\n");
        sb.append("    -D" + total_slots_per_clusterParam
                + " Should specify the total number of address bins for hashing the messages. Please read the Dempsy Users Guide for details. If not specified the default is "
                + total_slots_per_clusterDefault + "\n");
        sb.append("    -D" + min_num_nodes_per_clusterParam
                + " Should specify the minumum number of nodes required for this cluster. Please read the Dempsy Users Guide for details. If not specified the default is "
                + min_num_nodes_per_clusterDefault + "\n");

        logger.error(MarkerFactory.getMarker("FATAL"), sb.toString());
        throw new DempsyException(sb.toString());
    }

}
