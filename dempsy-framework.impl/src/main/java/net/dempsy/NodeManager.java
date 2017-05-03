package net.dempsy;

import static net.dempsy.Infrastructure.clusters;
import static net.dempsy.Infrastructure.nodes;

import java.util.ArrayList;
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
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.monitoring.NodeStatsCollector;
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
    private Receiver receiver = null;
    private final List<PerContainer> containers = new ArrayList<>();
    private final Map<ClusterId, Adaptor> adaptors = new HashMap<>();
    private Router router = null;
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
        if (session == null)
            throw new NullPointerException("Cannot pass a null collaborator to " + NodeManager.class.getSimpleName());
        if (this.session != null)
            throw new IllegalStateException("Collaborator session is already set on " + NodeManager.class.getSimpleName());
        this.session = session;
        return this;
    }

    private static Container makeContainer(final String containerTypeId) {
        return new Manager<>(Container.class).getAssociatedInstance(containerTypeId);
    }

    public NodeManager start() throws DempsyException {
        validate();

        nodeStatsCollector = tr.track((NodeStatsCollector) node.getNodeStatsCollector());

        // TODO: cleaner?
        statsCollectorFactory = tr.track(new Manager<>(ClusterStatsCollectorFactory.class)
                .getAssociatedInstance(node.getClusterStatsCollectorFactoryId()));

        // =====================================
        // set the dispatcher on adaptors and create containers for mp clusters
        final AtomicReference<String> firstAdaptorClusterName = new AtomicReference<>(null);
        node.getClusters().forEach(c -> {
            if (c.isAdaptor()) {
                if (firstAdaptorClusterName.get() == null)
                    firstAdaptorClusterName.set(c.getClusterId().clusterName);
                final Adaptor adaptor = c.getAdaptor();
                adaptors.put(c.getClusterId(), adaptor);
            } else {
                final Container con = makeContainer(node.getContainerTypeId()).setMessageProcessor(c.getMessageProcessor())
                        .setClusterId(c.getClusterId());

                // TODO: This is a hack for now.
                final Manager<RoutingStrategy.Inbound> inboundManager = new Manager<>(RoutingStrategy.Inbound.class);
                final RoutingStrategy.Inbound is = inboundManager.getAssociatedInstance(c.getRoutingStrategyId());
                containers.add(new PerContainer(con, is, c));
            }
        });
        // =====================================

        // =====================================
        // register node with session
        // =====================================
        // first gather node information

        // it we're all adaptor then don't bother to get the receiver.
        if (containers.size() == 0) {
            // here there's no point in a reciever since there's nothing to recieve.
            if (firstAdaptorClusterName.get() == null)
                throw new IllegalStateException("There seems to be no clusters or adaptors defined for this node \"" + node.toString() + "\"");
        } else {
            receiver = (Receiver) node.getReceiver();
            if (receiver != null) // otherwise we're all adaptor
                nodeAddress = receiver.getAddress();
            else if (firstAdaptorClusterName.get() == null)
                throw new IllegalStateException("There seems to be no clusters or adaptors defined for this node \"" + node.toString() + "\"");
        }

        nodeId = Optional.ofNullable(nodeAddress).map(n -> n.getGuid()).orElse(firstAdaptorClusterName.get());
        nodeStatsCollector.setNodeId(nodeId);

        if (nodeAddress == null && node.getReceiver() != null)
            LOGGER.warn("The node at " + nodeId + " contains no message processors but has a Reciever set. The receiver will never be started.");

        threading = tr.track(new DefaultThreadingModel("NodeThreadPool-" + nodeId + "-"))
                .setCoresFactor(1.0).setAdditionalThreads(1).setHardShutdown(true)
                .setMaxNumberOfQueuedLimitedTasks(10000).start();

        final NodeReceiver nodeReciever = receiver == null ? null : tr
                .track(new NodeReceiver(containers.stream().map(pc -> pc.container).collect(Collectors.toList()), threading, nodeStatsCollector));

        final Map<ClusterId, ClusterInformation> messageTypesByClusterId = new HashMap<>();
        containers.stream().map(pc -> pc.clusterDefinition).forEach(c -> {
            messageTypesByClusterId.put(c.getClusterId(),
                    new ClusterInformation(c.getRoutingStrategyId(), c.getClusterId(), c.getMessageProcessor().messagesTypesHandled()));
        });
        final NodeInformation nodeInfo = nodeAddress != null ? new NodeInformation(receiver.transportTypeId(), nodeAddress, messageTypesByClusterId)
                : null;

        // Then actually register the Node
        if (nodeInfo != null) {
            keepNodeRegstered = new PersistentTask(LOGGER, isRunning, persistenceScheduler, RETRY_PERIOND_MILLIS) {
                @Override
                public boolean execute() {
                    try {
                        final String application = node.application;
                        session.recursiveMkdir(clusters(application), null, DirMode.PERSISTENT, DirMode.PERSISTENT);
                        session.recursiveMkdir(nodes(application), null, DirMode.PERSISTENT, DirMode.PERSISTENT);

                        final String nodePath = nodes(application) + "/" + nodeId;

                        session.mkdir(nodePath, nodeInfo, DirMode.EPHEMERAL);
                        final NodeInformation reread = (NodeInformation) session.getData(nodePath, this);
                        final boolean ret = nodeInfo.equals(reread);
                        if (ret == true)
                            ptaskReady.set(true);
                        return ret;
                    } catch (final ClusterInfoException e) {
                        final String logmessage = "Failed to register the node. Retrying in " + RETRY_PERIOND_MILLIS + " milliseconds.";
                        if (LOGGER.isDebugEnabled())
                            LOGGER.info(logmessage, e);
                        else
                            LOGGER.info(logmessage);
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
        // Receiver -> NodeReceiver -> adaptor -> container -> Router -> RoutingStrategyOB -> Transport
        //
        // starting needs to happen in reverse.
        // =====================================
        isRunning.set(true);

        this.tManager = tr.start(new TransportManager(), this);
        this.rsManager = tr.start(new RoutingStrategyManager(), this);

        // create the router but don't start it yet.
        this.router = new Router(rsManager, nodeAddress, nodeReciever, tManager, nodeStatsCollector);

        // set up containers
        containers.forEach(pc -> pc.container.setDispatcher(router));

        // start containers
        containers.forEach(pc -> tr.start(pc.container, this));

        // set up adaptors
        adaptors.values().forEach(a -> a.setDispatcher(router));

        // IB routing strategy
        final int numContainers = containers.size();
        for (int i = 0; i < numContainers; i++) {
            final PerContainer c = containers.get(i);
            c.inboundStrategy.setContainerDetails(c.clusterDefinition.getClusterId(), new ContainerAddress(nodeAddress, i), c.container);
            tr.start(c.inboundStrategy, this);
        }

        // start router
        tr.start(this.router, this);

        final PersistentTask startAdaptorAfterRouterIsRunning = new PersistentTask(LOGGER, isRunning, this.persistenceScheduler, 500) {
            @Override
            public boolean execute() {
                if (!router.isReady())
                    return false;

                adaptors.entrySet().forEach(e -> threading.runDaemon(() -> tr.track(e.getValue()).start(), "Adaptor-" + e.getKey().clusterName));
                return true;
            }
        };

        // make sure the router is running. Once it is, start the adaptor(s)
        startAdaptorAfterRouterIsRunning.process();

        if (receiver != null)
            tr.track(receiver).start(nodeReciever, threading);

        tr.track(session); // close this session when we're done.
        // =====================================

        // make it known we're here and ready
        if (keepNodeRegstered != null)
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
    public void close() throws Exception {
        stop();
    }

    public List<Container> getContainers() {
        return Collections.unmodifiableList(containers.stream().map(pc -> pc.container).collect(Collectors.toList()));
    }

    public NodeManager validate() throws DempsyException {
        if (node == null)
            throw new DempsyException("No node set");

        node.validate();

        // do we have at least one message processor (or are we all adaptor)?
        boolean hasMessageProc = false;
        for (final Cluster c : node.getClusters()) {
            if (!c.isAdaptor()) {
                hasMessageProc = true;
                break;
            }
        }

        if (node.getReceiver() != null) {
            if (!Receiver.class.isAssignableFrom(node.getReceiver().getClass()))
                throw new DempsyException("The Node doesn't contain a " + Receiver.class.getSimpleName() + ". Instead it has a "
                        + SafeString.valueOfClass(node.getReceiver()));
        } else if (hasMessageProc)
            throw new DempsyException("Node has a message processor but no reciever.");

        // if session is non-null, then so is the Router.
        if (session == null)
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

    // Testing accessors

    // ==============================================================================
    // ++++++++++++++++++++++++++ STRICTLY FOR TESTING ++++++++++++++++++++++++++++++
    // ==============================================================================
    Router getRouter() {
        return router;
    }

    MessageProcessorLifecycle<?> getMp(final String clusterName) {
        final Cluster cluster = node.getClusters().stream().filter(c -> clusterName.equals(c.getClusterId().clusterName)).findAny().orElse(null);
        return cluster == null ? null : cluster.getMessageProcessor();
    }

    Container getContainer(final String clusterName) {
        return containers.stream().filter(pc -> clusterName.equals(pc.clusterDefinition.getClusterId().clusterName)).findFirst().get().container;
    }

    // ==============================================================================

    private static class PerContainer {
        final Container container;
        final RoutingStrategy.Inbound inboundStrategy;
        final Cluster clusterDefinition;

        public PerContainer(final Container container, final Inbound inboundStrategy, final Cluster clusterDefinition) {
            this.container = container;
            this.inboundStrategy = inboundStrategy;
            this.clusterDefinition = clusterDefinition;
        }
    }

    public Node getNode() {
        return node;
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
