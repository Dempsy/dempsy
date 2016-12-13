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

package net.dempsy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.config.ApplicationDefinition;
import net.dempsy.config.ClusterDefinition;
import net.dempsy.config.ClusterId;
import net.dempsy.container.MpContainer;
import net.dempsy.executor.DempsyExecutor;
import net.dempsy.internal.util.SafeString;
import net.dempsy.messagetransport.Destination;
import net.dempsy.messagetransport.Receiver;
import net.dempsy.messagetransport.Transport;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.monitoring.StatsCollectorFactory;
import net.dempsy.output.OutputExecuter;
import net.dempsy.router.CurrentClusterCheck;
import net.dempsy.router.Router;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.serialization.Serializer;

/**
 * This class is the master orchestrator of the setup of a VM running the application. It is intended to have the ApplicationDefinitions autowired in so it picks up each one.
 */
public class Dempsy {
    static Logger logger = LoggerFactory.getLogger(Dempsy.class);

    /**
     * There is an instance of an application for every ApplicationDefinition autowired into the Dempsy instance. In most real cases this will be only one.
     */
    public class Application {
        /**
         * Currently a "Dempsy.Application.Cluster" has no utility since it only has a single Node. This may change
         */
        public class Cluster {
            /**
             * A Node is essentially an {@link MpContainer} with all of the attending infrastructure. Currently a Node is instantiated within the Dempsy orchestrator as a one to one with the {@link Cluster}.
             */
            public class Node {
                protected ClusterDefinition clusterDefinition;

                Router router = null;
                private MpContainer container = null;
                RoutingStrategy.Inbound strategyInbound = null;
                List<Class<?>> acceptedMessageClasses = null;
                Receiver receiver = null;
                StatsCollector statsCollector = null;

                private Node(final ClusterDefinition clusterDefinition) {
                    this.clusterDefinition = clusterDefinition;
                }

                private void start() throws DempsyException {
                    try {
                        final DempsyExecutor executor = (DempsyExecutor) clusterDefinition.getExecutor(); // this can be null
                        if (executor != null)
                            executor.start();
                        final ClusterInfoSession clusterSession = clusterSessionFactory.createSession();
                        final ClusterId currentClusterId = clusterDefinition.getClusterId();
                        router = new Router(clusterDefinition.getParentApplicationDefinition());
                        router.setCurrentCluster(currentClusterId);
                        router.setClusterSession(clusterSession);
                        // get the executor if one is set

                        container = new MpContainer(currentClusterId);
                        container.setDispatcher(router);
                        if (executor != null)
                            container.setExecutor(executor);
                        final Object messageProcessorPrototype = clusterDefinition.getMessageProcessorPrototype();
                        if (messageProcessorPrototype != null)
                            container.setPrototype(messageProcessorPrototype);
                        acceptedMessageClasses = getAcceptedMessages(clusterDefinition);

                        final Serializer serializer = (Serializer) clusterDefinition.getSerializer();
                        if (serializer != null)
                            container.setSerializer(serializer);

                        // there is only a reciever if we have an Mp (that is, we aren't an adaptor) and start accepting messages
                        Destination thisDestination = null;
                        if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0) {
                            receiver = transport.createInbound(executor);
                            receiver.setListener(container);
                            receiver.start();
                            thisDestination = receiver.getDestination();
                        }

                        final StatsCollectorFactory statsFactory = (StatsCollectorFactory) clusterDefinition.getStatsCollectorFactory();
                        if (statsFactory != null) {
                            statsCollector = statsFactory.createStatsCollector(currentClusterId,
                                    receiver != null ? thisDestination : null);
                            router.setStatsCollector(statsCollector);
                            container.setStatCollector(statsCollector);
                            if (receiver != null)
                                receiver.setStatsCollector(statsCollector);
                        }

                        router.setDefaultSenderFactory(transport.createOutbound(executor, statsCollector));

                        final RoutingStrategy strategy = (RoutingStrategy) clusterDefinition.getRoutingStrategy();

                        final KeySource<?> keySource = clusterDefinition.getKeySource();
                        if (keySource != null)
                            container.setKeySource(keySource);

                        // there is only an inbound strategy if we have an Mp (that is, we aren't an adaptor) and
                        // we actually accept messages
                        if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0)
                            strategyInbound = strategy.createInbound(clusterSession, currentClusterId, acceptedMessageClasses, thisDestination,
                                    container);

                        // this can fail because of down cluster manager server ... but it should eventually recover.
                        try {
                            router.initialize();
                        } catch (final ClusterInfoException e) {
                            logger.warn("Strategy failed to initialize. Continuing anyway. The cluster manager issue will be resolved automatically.",
                                    e);
                        }

                        final Adaptor adaptor = clusterDefinition.isRouteAdaptorType() ? clusterDefinition.getAdaptor() : null;
                        if (adaptor != null)
                            adaptor.setDispatcher(router);
                        else {
                            final OutputExecuter outputExecuter = (OutputExecuter) clusterDefinition.getOutputExecuter();
                            if (outputExecuter != null) {
                                outputExecuter.setOutputInvoker(container);
                            }
                        }

                        container.startEvictionThread(Cluster.this.clusterDefinition.getEvictionFrequency(),
                                Cluster.this.clusterDefinition.getEvictionTimeUnit());

                    } catch (final RuntimeException e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new DempsyException(e);
                    }
                }

                public StatsCollector getStatsCollector() {
                    return statsCollector;
                }

                public MpContainer getMpContainer() {
                    return container;
                }

                public void stop() {
                    if (receiver != null) {
                        try {
                            receiver.stop();
                            receiver = null;
                        } catch (final Throwable th) {
                            logger.error("Error stoping the reciever " + SafeString.objectDescription(receiver) +
                                    " for " + SafeString.valueOf(clusterDefinition) + " due to the following exception:", th);
                        }
                    }

                    // shut the container down prior to the router.
                    if (container != null)
                        try {
                            container.shutdown();
                            container = null;
                        } catch (final Throwable th) {
                            logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th);
                        }

                    // the cluster session is stopped by the router.
                    if (router != null)
                        try {
                            router.stop();
                            router = null;
                        } catch (final Throwable th) {
                            logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th);
                        }

                    if (statsCollector != null)
                        try {
                            statsCollector.stop();
                            statsCollector = null;
                        } catch (final Throwable th) {
                            logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th);
                        }

                    if (strategyInbound != null)
                        try {
                            strategyInbound.stop();
                            strategyInbound = null;
                        } catch (final Throwable th) {
                            logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th);
                        }

                    final DempsyExecutor executor = (DempsyExecutor) clusterDefinition.getExecutor(); // this can be null
                    if (executor != null)
                        try {
                            executor.shutdown();
                        } catch (final Throwable th) {
                            logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th);
                        }
                }

                // Only called from tests
                public Router retouRteg() {
                    return router;
                }

            } // end Node definition

            private final List<Node> nodes = new ArrayList<Node>(1);
            protected ClusterDefinition clusterDefinition;

            private Cluster(final ClusterDefinition clusterDefinition) {
                this.clusterDefinition = clusterDefinition;
                allClusters.put(clusterDefinition.getClusterId(), this);
            }

            private void start() throws DempsyException {
                nodes.clear();
                final Node node = new Node(clusterDefinition);
                nodes.add(node);
                node.start();
            }

            private void stop() {
                for (final Node node : nodes)
                    node.stop();
            }

            public List<Node> getNodes() {
                return nodes;
            }

            /**
             * This is only public for testing - DO NOT CALL!
             * 
             * @throws DempsyException
             */
            public void instantiateAndStartAnotherNodeForTesting() throws DempsyException {
                final Node node = new Node(clusterDefinition);
                nodes.add(node);
                node.start();
            }

        } // end Cluster Definition

        protected ApplicationDefinition applicationDefinition;
        protected List<Cluster> appClusters = new ArrayList<Cluster>();
        private final List<AdaptorThread> adaptorThreads = new ArrayList<AdaptorThread>();

        public Application(final ApplicationDefinition applicationDefinition) {
            this.applicationDefinition = applicationDefinition;
        }

        private DempsyException failedStart = null;

        /**
         * 
         * @return boolean - true if starts the cluster, else false
         * @throws DempsyException
         */
        public boolean start() throws DempsyException {
            failedStart = null;
            boolean clusterStarted = false;

            for (final ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions()) {
                if (clusterCheck.isThisNodePartOfCluster(clusterDef.getClusterId())) {
                    final Cluster cluster = new Cluster(clusterDef);
                    appClusters.add(cluster);
                }
            }

            final List<Thread> toJoin = new ArrayList<Thread>(appClusters.size());

            for (final Cluster cluster : appClusters) {
                // if (multiThreadedStart)
                // {
                // Thread t = new Thread(new ClusterStart(cluster),"Starting cluster:" + cluster.clusterDefinition);
                // toJoin.add(t);
                // t.start();
                // clusterStarted = true;
                // }
                // else
                // {
                cluster.start();
                clusterStarted = true;
                // }
            }

            for (final Thread t : toJoin) {
                try {
                    t.join();
                } catch (final InterruptedException e) { /* just continue */ }
            }

            if (failedStart != null)
                throw failedStart;

            for (final Cluster cluster : appClusters) {
                if (clusterCheck.isThisNodePartOfCluster(cluster.clusterDefinition.getClusterId())) {
                    final Adaptor adaptor = cluster.clusterDefinition.getAdaptor();
                    if (adaptor != null) {
                        final AdaptorThread adaptorRunnable = new AdaptorThread(adaptor);
                        final Thread thread = new Thread(adaptorRunnable, "Adaptor - " + SafeString.objectDescription(adaptor));
                        adaptorThreads.add(adaptorRunnable);
                        if (cluster.clusterDefinition.isAdaptorDaemon())
                            thread.setDaemon(true);
                        thread.start();
                        clusterStarted = true;
                    } else {
                        final OutputExecuter outputExecuter = (OutputExecuter) cluster.clusterDefinition.getOutputExecuter();
                        if (outputExecuter != null) {
                            outputExecuter.start();
                        }
                    }
                }
            }
            return clusterStarted;
        }

        public void stop() {
            // first stop all of the adaptor threads
            for (final AdaptorThread adaptorThread : adaptorThreads)
                adaptorThread.stop();

            // stop all of the non-adaptor clusters.
            for (final Cluster cluster : appClusters) {
                final OutputExecuter outputExecuter = (OutputExecuter) cluster.clusterDefinition.getOutputExecuter();
                if (outputExecuter != null) {
                    outputExecuter.stop();
                }
                cluster.stop();
            }
        }

    } // end Application definition

    private static class AdaptorThread implements Runnable {
        private Adaptor adaptor = null;
        private Thread thread = null;

        public AdaptorThread(final Adaptor adaptor) {
            this.adaptor = adaptor;
        }

        @Override
        public void run() {
            thread = Thread.currentThread();
            logger.info("Starting adaptor thread for " + SafeString.objectDescription(adaptor));
            try {
                adaptor.start();
            } catch (final Throwable th) {
                logger.warn("Adaptor " + SafeString.objectDescription(adaptor) + " threw an unexpected exception.", th);
            } finally {
                logger.info("Adaptor thread for " + SafeString.objectDescription(adaptor) + " is shutting down");
            }

        }

        /**
         * This is a helper method that should stop the running thread by initiating a stop on the adaptor, then interrupting the thread in case it's in a blocking call somewhere.
         */
        public void stop() {
            try {
                if (adaptor != null)
                    adaptor.stop();
            } catch (final Throwable th) {
                logger.error("Problem trying to stop the Adaptor " + SafeString.objectDescription(adaptor), th);
            }
            if (thread != null)
                thread.interrupt();
        }
    }

    private List<ApplicationDefinition> applicationDefinitions = null;
    protected List<Application> applications = null;
    private CurrentClusterCheck clusterCheck = null;
    protected ClusterInfoSessionFactory clusterSessionFactory = null;
    private RoutingStrategy defaultRoutingStrategy = null;
    private Serializer defaultSerializer = null;
    private Transport transport = null;
    private final Map<ClusterId, Application.Cluster> allClusters = new HashMap<ClusterId, Application.Cluster>();
    private StatsCollectorFactory defaultStatsCollectorFactory = null;

    // Dempsy lifecycle state
    private volatile boolean isRunning = false;
    private final Object isRunningEvent = new Object();

    // // this is mainly for testing purposes.
    // private boolean multiThreadedStart = false;

    public Dempsy() {}

    /**
     * This is meant to be autowired by type.
     */
    public void setApplicationDefinitions(final List<ApplicationDefinition> applicationDefinitions) {
        this.applicationDefinitions = applicationDefinitions;
    }

    public synchronized void start() throws DempsyException {
        if (isRunning())
            throw new DempsyException("The Dempsy application " + applicationDefinitions + " has already been started.");

        if (applicationDefinitions == null || applicationDefinitions.size() == 0)
            throw new DempsyException("Cannot start this application because there are no ApplicationDefinitions");

        if (clusterSessionFactory == null)
            throw new DempsyException("Cannot start this application because there was no ClusterFactory implementaiton set.");

        if (clusterCheck == null)
            throw new DempsyException(
                    "Cannot start this application because there's no way to tell which cluster to start. Make sure the appropriate " +
                            CurrentClusterCheck.class.getSimpleName() + " is set.");

        if (defaultRoutingStrategy == null)
            throw new DempsyException("Cannot start this application because there's no default routing strategy defined.");

        if (defaultSerializer == null)
            throw new DempsyException("Cannot start this application because there's no default serializer defined.");

        if (transport == null)
            throw new DempsyException("Cannot start this application because there's no transport implementation defined");

        if (defaultStatsCollectorFactory == null)
            throw new DempsyException("Cannot start this application because there's no default stats collector factory defined.");

        try {
            applications = new ArrayList<Application>(applicationDefinitions.size());
            for (final ApplicationDefinition appDef : this.applicationDefinitions) {
                appDef.initialize();
                if (clusterCheck.isThisNodePartOfApplication(appDef.getApplicationName())) {
                    final Application app = new Application(appDef);

                    // set the default routing strategy if there isn't one already set.
                    if (appDef.getRoutingStrategy() == null)
                        appDef.setRoutingStrategy(defaultRoutingStrategy);

                    if (appDef.getSerializer() == null)
                        appDef.setSerializer(defaultSerializer);

                    if (appDef.getStatsCollectorFactory() == null)
                        appDef.setStatsCollectorFactory(defaultStatsCollectorFactory);

                    applications.add(app);
                }
            }

            boolean clusterStarted = false;
            for (final Application app : applications)
                clusterStarted = app.start();

            if (!clusterStarted) {
                throw new DempsyException("Cannot start this application because cluster defination was not found.");
            }
            // if we got to here we can assume we're started
            synchronized (isRunningEvent) {
                isRunning = true;
            }
        } catch (final RuntimeException rte) {
            logger.error("Failed to start Dempsy. Attempting to stop.");
            // if something unpexpected happened then we should attempt to stop
            try {
                stop();
            } catch (final Throwable th) {}
            throw rte;
        }
    }

    public synchronized void stop() {
        try {
            if (applications != null) {
                for (final Application app : applications)
                    app.stop();
            }
        } finally {
            // even though we may have had an exception, there's no way Dempsy
            // can be considered still "running."
            synchronized (isRunningEvent) {
                isRunning = false;
                isRunningEvent.notifyAll();
            }
        }
    }

    public ClusterInfoSessionFactory getClusterSessionFactory() {
        return clusterSessionFactory;
    }

    public void setClusterSessionFactory(final ClusterInfoSessionFactory clusterFactory) {
        this.clusterSessionFactory = clusterFactory;
    }

    public void setClusterCheck(final CurrentClusterCheck clusterCheck) {
        this.clusterCheck = clusterCheck;
    }

    // public void setMultithreadedStart(boolean multiThreadedStart) { this.multiThreadedStart = multiThreadedStart; }

    public void setDefaultTransport(final Transport transport) {
        this.transport = transport;
    }

    public void setDefaultRoutingStrategy(final RoutingStrategy defaultRoutingStrategy) {
        this.defaultRoutingStrategy = defaultRoutingStrategy;
    }

    public void setDefaultSerializer(final Serializer defaultSerializer) {
        this.defaultSerializer = defaultSerializer;
    }

    public void setDefaultStatsCollectorFactory(final StatsCollectorFactory defaultfactory) {
        this.defaultStatsCollectorFactory = defaultfactory;
    }

    public Application.Cluster getCluster(final ClusterId clusterId) {
        return allClusters.get(clusterId);
    }

    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Wait for Dempsy to be stopped. This is useful in a 'main' that needs to wait for an external shutdown to complete.
     * 
     * @throws InterruptedException
     *             if the waiting was interrupted.
     */
    public void waitToBeStopped() throws InterruptedException {
        waitToBeStopped(-1);
    }

    /**
     * Wait for Dempsy to be stopped for the specified time.
     * 
     * @return true if Dempsy actually stopped. false if the timeout was reached.
     * @throws InterruptedException
     *             if the waiting was interrupted.
     */
    public boolean waitToBeStopped(final long timeInMillis) throws InterruptedException {
        synchronized (isRunningEvent) {
            while (isRunning) {
                if (timeInMillis < 0)
                    isRunningEvent.wait();
                else
                    isRunningEvent.wait(timeInMillis);
            }
            return !isRunning();
        }
    }

    protected static List<Class<?>> getAcceptedMessages(final ClusterDefinition clusterDef) {
        final List<Class<?>> messageClasses = new ArrayList<Class<?>>();
        final Object prototype = clusterDef.getMessageProcessorPrototype();
        if (prototype != null) {
            for (final Method method : prototype.getClass().getMethods()) {
                if (method.isAnnotationPresent(net.dempsy.annotations.MessageHandler.class)) {
                    for (final Class<?> messageType : method.getParameterTypes()) {
                        messageClasses.add(messageType);
                    }
                }
            }
        }
        return messageClasses;
    }
}
