package net.dempsy;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.NodeAddress;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public class TestInfrastructure implements Infrastructure, AutoCloseable {
    final ClusterInfoSession session;
    final AutoDisposeSingleThreadScheduler sched;
    final ClusterStatsCollectorFactory statsFact;
    final NodeStatsCollector nodeStats;
    final String application;
    final ThreadingModel threading;

    public TestInfrastructure(final String testName, final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched,
        final ThreadingModel threading) {
        this.session = session;
        this.sched = sched;
        statsFact = new ClusterStatsCollectorFactory() {

            @Override
            public void close() throws Exception {}

            @Override
            public ClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress nodeIdentification) {
                return null;
            }

        };
        nodeStats = new NodeStatsCollector() {

            @Override
            public void stop() {}

            @Override
            public void setNodeId(final String nodeId) {}

            @Override
            public void messageReceived(final Object message) {}

            @Override
            public void messageDiscarded(final Object message) {}

            @Override
            public void messageSent(final Object message) {}

            @Override
            public void messageNotSent() {}

            @Override
            public void setMessagesPendingGauge(final LongSupplier currentMessagesPendingGauge) {}

            @Override
            public void setMessagesOutPendingGauge(final LongSupplier currentMessagesOutPendingGauge) {}

        };
        this.application = testName;
        this.threading = threading;
    }

    public TestInfrastructure(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        this("application", session, sched, null);
    }

    public TestInfrastructure(final String testName, final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        this(testName, session, sched, null);
    }

    public TestInfrastructure(final ClusterId cid, final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        this(cid.applicationName, session, sched, null);
    }

    public TestInfrastructure(final ThreadingModel threading) {
        this("application", null, null, threading);
    }

    @Override
    public ClusterInfoSession getCollaborator() {
        return session;
    }

    @Override
    public AutoDisposeSingleThreadScheduler getScheduler() {
        return sched;
    }

    @Override
    public RootPaths getRootPaths() {
        return new RootPaths("/" + application, "/" + application + "/nodes", "/" + application + "/clusters");
    }

    @Override
    public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
        return statsFact.createStatsCollector(clusterId, null);
    }

    @Override
    public Map<String, String> getConfiguration() {
        return new HashMap<>();
    }

    @Override
    public NodeStatsCollector getNodeStatsCollector() {
        return nodeStats;
    }

    @Override
    public String getNodeId() {
        return "test-infrastructure-fake-node-id";
    }

    @Override
    public ThreadingModel getThreadingModel() {
        return threading;
    }

    @Override
    public Node getNode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if(threading != null)
            threading.close();
    }
}
