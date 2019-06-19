package net.dempsy;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;

public class DispatcherFactory {

    private final NodeManager nodeManager;
    private Dispatcher dispatcher = null;

    private class DummyAdaptor implements Adaptor {
        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            DispatcherFactory.this.dispatcher = dispatcher;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    @SuppressWarnings("resource")
    public DispatcherFactory(final Cluster clusterDef, final ClusterInfoSessionFactory sessionFactory, final String appName) throws ClusterInfoException {
        clusterDef.setAdaptor(new DummyAdaptor());

        nodeManager = new NodeManager()
            .node(
                new Node.Builder(appName)
                    .clusters(clusterDef)
                    .build())
            .collaborator(sessionFactory.createSession())
            .start();
    }

    public void close() {
        nodeManager.close();
    }

    public Dispatcher dispatcher() {
        return dispatcher;
    }
}
