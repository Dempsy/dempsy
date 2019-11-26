package net.dempsy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.container.Container;
import net.dempsy.intern.OutgoingDispatcher;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.monitoring.dummy.DummyNodeStatsCollector;
import net.dempsy.output.OutputScheduler;
import net.dempsy.router.RoutingInboundManager;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.TransportManager;
import net.dempsy.util.SafeString;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;
import net.dempsy.utils.PersistentTask;

public class NodeManager implements Infrastructure, AutoCloseable {
    private final static Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);
    private static final long RETRY_PERIOND_MILLIS = 500L;

    private Node node = null;
    private ClusterInfoSession session;
    private final AutoDisposeSingleThreadScheduler persistenceScheduler = new AutoDisposeSingleThreadScheduler("dempsy-pestering");

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final ServiceTracker tr = new ServiceTracker();

    private ThreadingModel threading;

    // created in start(). Stopped in stop()
    private final List<PerContainer> containers = new ArrayList<>();
    private final Map<ClusterId, Adaptor> adaptors = new HashMap<>();

    // non-final fields.
    private Receiver receiver = null;
    private OutgoingDispatcher router = null;
    private PersistentTask keepNodeRegstered = null;
    private RootPaths rootPaths = null;
    private ClusterStatsCollectorFactory statsCollectorFactory;
    private NodeStatsCollector nodeStatsCollector;
    private RoutingStrategyManager rsManager = null;
    private TransportManager tManager = null;
    private NodeAddress nodeAddress = null;
    private String nodeId = null;
    AtomicBoolean ptaskReady = new AtomicBoolean(false);

    public NodeManager() {}

    public NodeManager(final Node node, final ClusterInfoSessionFactory sessionFactory) throws ClusterInfoException {
        node(node);
        collaborator(sessionFactory.createSession());
    }

    public NodeManager node(final Node node) {
        this.node = node;
        final String appName = node.application;
        this.rootPaths = new RootPaths(appName);
        return this;
    }

    public NodeManager collaborator(final ClusterInfoSession session) {
        if(session == null)
            throw new NullPointerException("Cannot pass a null collaborator to " + NodeManager.class.getSimpleName());
        if(this.session != null)
            throw new IllegalStateException("Collaborator session is already set on " + NodeManager.class.getSimpleName());
        this.session = session;
        return this;
    }

    /**
     * This methods expects that the thread manager will be started and stopped apart from the NodeManager itself.
     */
    public NodeManager threadingModel(final ThreadingModel threading) {
        this.threading = threading;
        return this;
    }

    private static Container makeContainer(final String containerTypeId) {
        return new Manager<>(Container.class).getAssociatedInstance(containerTypeId);
    }

    public NodeManager start() throws DempsyException {
        validate();

        nodeStatsCollector = tr.track((NodeStatsCollector)node.getNodeStatsCollector());

        // TODO: cleaner?
        statsCollectorFactory = tr.track(new Manager<>(ClusterStatsCollectorFactory.class)
            .getAssociatedInstance(node.getClusterStatsCollectorFactoryId()));

        // =====================================
        // set the dispatcher on adaptors and create containers for mp clusters
        final AtomicReference<String> firstAdaptorClusterName = new AtomicReference<>(null);
        node.getClusters().forEach(c -> {
            if(c.isAdaptor()) {
                if(firstAdaptorClusterName.get() == null)
                    firstAdaptorClusterName.set(c.getClusterId().clusterName);
                final Adaptor adaptor = c.getAdaptor();
                adaptors.put(c.getClusterId(), adaptor);

                if(c.getRoutingStrategyId() != null && !"".equals(c.getRoutingStrategyId().trim()) && !" ".equals(c.getRoutingStrategyId().trim()))
                    LOGGER.warn("The cluster " + c.getClusterId()
                        + " contains an adaptor but also has the routingStrategy set. The routingStrategy will be ignored.");

                if(c.getOutputScheduler() != null)
                    LOGGER.warn("The cluster " + c.getClusterId()
                        + " contains an adaptor but also has an output executor set. The output executor will never be used.");
            } else {
                String containerTypeId = c.getContainerTypeId();
                if(containerTypeId == null)
                    containerTypeId = node.getContainerTypeId(); // can't be null
                final Container con = makeContainer(containerTypeId).setMessageProcessor(c.getMessageProcessor())
                    .setClusterId(c.getClusterId()).setMaxPendingMessagesPerContainer(c.getMaxPendingMessagesPerContainer());

                // TODO: This is a hack for now.
                final Manager<RoutingStrategy.Inbound> inboundManager = new RoutingInboundManager();
                final RoutingStrategy.Inbound is = inboundManager.getAssociatedInstance(c.getRoutingStrategyId());

                final boolean outputSupported = c.getMessageProcessor().isOutputSupported();
                final Object outputScheduler = c.getOutputScheduler();

                // if there mp handles output but there's no output scheduler then we should warn.
                if(outputSupported && outputScheduler == null)
                    LOGGER.warn("The cluster " + c.getClusterId()
                        + " contains a message processor that supports an output cycle but there's no executor set so it will never be invoked.");

                if(!outputSupported && outputScheduler != null)
                    LOGGER.warn("The cluster " + c.getClusterId()
                        + " contains a message processor that doesn't support an output cycle but there's an output cycle executor set. The output cycle executor will never be used.");

                final OutputScheduler os = (outputSupported && outputScheduler != null) ? (OutputScheduler)outputScheduler : null;
                containers.add(new PerContainer(con, is, c, os));
            }
        });
        // =====================================

        // =====================================
        // register node with session
        // =====================================
        // first gather node information

        // it we're all adaptor then don't bother to get the receiver.
        if(containers.size() == 0) {
            // here there's no point in a receiver since there's nothing to receive.
            if(firstAdaptorClusterName.get() == null)
                throw new IllegalStateException("There seems to be no clusters or adaptors defined for this node \"" + node.toString() + "\"");
        } else {
            receiver = (Receiver)node.getReceiver();
            if(receiver != null) // otherwise we're all adaptor
                nodeAddress = receiver.getAddress(this);
            else if(firstAdaptorClusterName.get() == null)
                throw new IllegalStateException("There seems to be no clusters or adaptors defined for this node \"" + node.toString() + "\"");
        }

        nodeId = Optional.ofNullable(nodeAddress).map(n -> n.getGuid()).orElse(firstAdaptorClusterName.get());
        if(nodeStatsCollector == null) {
            LOGGER.warn("There is no {} set for the the application '{}'", StatsCollector.class.getSimpleName(), node.application);
            nodeStatsCollector = new DummyNodeStatsCollector();
        }

        nodeStatsCollector.setNodeId(nodeId);

        if(nodeAddress == null && node.getReceiver() != null)
            LOGGER.warn("The node at " + nodeId + " contains no message processors but has a Reciever set. The receiver will never be started.");

        if(nodeAddress == null && node.getDefaultRoutingStrategyId() != null)
            LOGGER.warn("The node at " + nodeId
                + " contains no message processors but has a defaultRoutingStrategyId set. The routingStrategyId will never be used.");

        if(threading == null)
            threading = tr.track(new DefaultThreadingModel("NodeThreadPool-" + nodeId + "-")).configure(node.getConfiguration()).start();

        nodeStatsCollector.setMessagesPendingGauge(() -> threading.getNumberLimitedPending());

        final NodeReceiver nodeReciever = receiver == null ? null
            : tr
                .track(new NodeReceiver(containers.stream().map(pc -> pc.container).collect(Collectors.toList()), threading,
                    nodeStatsCollector));

        final Map<ClusterId, ClusterInformation> messageTypesByClusterId = new HashMap<>();
        containers.stream().map(pc -> pc.clusterDefinition).forEach(c -> {
            messageTypesByClusterId.put(c.getClusterId(),
                new ClusterInformation(c.getRoutingStrategyId(), c.getClusterId(), c.getMessageProcessor().messagesTypesHandled()));
        });
        final NodeInformation nodeInfo = nodeAddress != null ? new NodeInformation(receiver.transportTypeId(), nodeAddress, messageTypesByClusterId)
            : null;

        // Then actually register the Node
        if(nodeInfo != null) {
            keepNodeRegstered = new PersistentTask(LOGGER, isRunning, persistenceScheduler, RETRY_PERIOND_MILLIS) {
                @Override
                public boolean execute() {
                    try {
                        session.recursiveMkdir(rootPaths.clustersDir, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
                        session.recursiveMkdir(rootPaths.nodesDir, null, DirMode.PERSISTENT, DirMode.PERSISTENT);

                        final String nodePath = rootPaths.nodesDir + "/" + nodeId;

                        session.mkdir(nodePath, nodeInfo, DirMode.EPHEMERAL);
                        final NodeInformation reread = (NodeInformation)session.getData(nodePath, this);
                        final boolean ret = nodeInfo.equals(reread);
                        if(ret == true)
                            ptaskReady.set(true);
                        return ret;
                    } catch(final ClusterInfoException e) {
                        final String logmessage = "Failed to register the node. Retrying in " + RETRY_PERIOND_MILLIS + " milliseconds.";
                        if(LOGGER.isDebugEnabled())
                            LOGGER.info(logmessage, e);
                        else
                            LOGGER.info(logmessage, e);
                    }
                    return false;
                }

                @Override
                public String toString() {
                    return "register node information";
                }
            };
        }

        // =====================================
        // The layering works this way.
        //
        // Receiver -> NodeReceiver -> adaptor -> container -> OutgoingDispatcher -> RoutingStrategyOB -> Transport
        //
        // starting needs to happen in reverse.
        // =====================================
        isRunning.set(true);

        this.tManager = tr.start(new TransportManager(), this);
        this.rsManager = tr.start(new RoutingStrategyManager(), this);

        // create the router but don't start it yet.
        this.router = new OutgoingDispatcher(rsManager, nodeAddress, nodeId, nodeReciever, tManager, nodeStatsCollector);

        // set up containers
        containers.forEach(pc -> pc.container.setDispatcher(router)
            .setEvictionCycle(pc.clusterDefinition.getEvictionFrequency().evictionFrequency,
                pc.clusterDefinition.getEvictionFrequency().evictionTimeUnit));

        // IB routing strategy
        final int numContainers = containers.size();
        for(int i = 0; i < numContainers; i++) {
            final PerContainer c = containers.get(i);
            c.inboundStrategy.setContainerDetails(c.clusterDefinition.getClusterId(), new ContainerAddress(nodeAddress, i), c.container);
        }

        // setup the output executors by passing the containers
        containers.stream().filter(pc -> pc.outputScheduler != null).forEach(pc -> pc.outputScheduler.setOutputInvoker(pc.container));

        // set up adaptors
        adaptors.values().forEach(a -> a.setDispatcher(router));

        // start containers after setting inbound
        containers.forEach(pc -> tr.start(pc.container.setInbound(pc.inboundStrategy), this));

        // start the output schedulers now that the containers have been started.
        containers.stream().map(pc -> pc.outputScheduler).filter(os -> os != null).forEach(os -> tr.start(os, this));

        // start IB routing strategy
        containers.forEach(pc -> tr.start(pc.inboundStrategy, this));

        // start router
        tr.start(this.router, this);

        final PersistentTask startAdaptorAfterRouterIsRunning = new PersistentTask(LOGGER, isRunning, this.persistenceScheduler, 500) {
            @Override
            public boolean execute() {
                if(!router.isReady())
                    return false;

                adaptors.entrySet().forEach(e -> threading.runDaemon(() -> tr.track(e.getValue()).start(), "Adaptor-" + e.getKey().clusterName));
                return true;
            }
        };

        // make sure the router is running. Once it is, start the adaptor(s)
        startAdaptorAfterRouterIsRunning.process();

        if(receiver != null)
            tr.track(receiver).start(nodeReciever, this);

        tr.track(session); // close this session when we're done.
        // =====================================

        // make it known we're here and ready
        if(keepNodeRegstered != null)
            keepNodeRegstered.process();
        else
            ptaskReady.set(true);

        return this;
    }

    public void stop() {
        isRunning.set(false);

        tr.stopAll();
    }

    public boolean isReady() {
        final boolean ret = ptaskReady.get() && tr.allReady();
        return ret;
    }

    @Override
    public void close() {
        stop();
    }

    public List<Container> getContainers() {
        return Collections.unmodifiableList(containers.stream().map(pc -> pc.container).collect(Collectors.toList()));
    }

    public NodeManager validate() throws DempsyException {
        if(node == null)
            throw new DempsyException("No node set");

        node.validate();

        // do we have at least one message processor (or are we all adaptor)?
        boolean hasMessageProc = false;
        for(final Cluster c: node.getClusters()) {
            if(!c.isAdaptor()) {
                hasMessageProc = true;
                break;
            }
        }

        if(node.getReceiver() != null) {
            if(!Receiver.class.isAssignableFrom(node.getReceiver().getClass()))
                throw new DempsyException("The Node doesn't contain a " + Receiver.class.getSimpleName() + ". Instead it has a "
                    + SafeString.valueOfClass(node.getReceiver()));
        } else if(hasMessageProc)
            throw new DempsyException("Node has a message processor but no reciever.");

        // if session is non-null, then so is the Router.
        if(session == null)
            throw new DempsyException("There's no collaborator set for this \"" + NodeManager.class.getSimpleName() + "\" ");

        return this;
    }

    @Override
    public ClusterInfoSession getCollaborator() {
        return session;
    }

    @Override
    public AutoDisposeSingleThreadScheduler getScheduler() {
        return persistenceScheduler;
    }

    @Override
    public RootPaths getRootPaths() {
        return rootPaths;
    }

    @Override
    public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
        return statsCollectorFactory.createStatsCollector(clusterId, nodeAddress);
    }

    @Override
    public NodeStatsCollector getNodeStatsCollector() {
        return nodeStatsCollector;
    }

    @Override
    public Map<String, String> getConfiguration() {
        return node.getConfiguration();
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public ThreadingModel getThreadingModel() {
        return threading;
    }

    @Override
    public Node getNode() {
        return node;
    }

    // Testing accessors

    // ==============================================================================
    // ++++++++++++++++++++++++++ STRICTLY FOR TESTING ++++++++++++++++++++++++++++++
    // ==============================================================================
    OutgoingDispatcher getRouter() {
        return router;
    }

    MessageProcessorLifecycle<?> getMp(final String clusterName) {
        final Cluster cluster = node.getClusters().stream().filter(c -> clusterName.equals(c.getClusterId().clusterName)).findAny().orElse(null);
        return cluster == null ? null : cluster.getMessageProcessor();
    }

    Container getContainer(final String clusterName) {
        return containers.stream().filter(pc -> clusterName.equals(pc.clusterDefinition.getClusterId().clusterName)).findFirst().get().container;
    }

    public Collection<ContainerAddress> getReachableContainers(final String clusterName) {
        return getRouter().allReachable(clusterName);
    }

    // ==============================================================================

    private static class PerContainer {
        final Container container;
        final RoutingStrategy.Inbound inboundStrategy;
        final Cluster clusterDefinition;
        final OutputScheduler outputScheduler;

        public PerContainer(final Container container, final Inbound inboundStrategy, final Cluster clusterDefinition, final OutputScheduler os) {
            this.container = container;
            this.inboundStrategy = inboundStrategy;
            this.clusterDefinition = clusterDefinition;
            this.outputScheduler = os;
        }
    }

    public void setNode(final Node node) {
        node(node);
    }

    public ClusterInfoSession getSession() {
        return session;
    }

    public void setSession(final ClusterInfoSession session) {
        collaborator(session);
    }

}
