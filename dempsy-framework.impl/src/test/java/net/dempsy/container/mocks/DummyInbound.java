package net.dempsy.container.mocks;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;

public class DummyInbound implements RoutingStrategy.Inbound {

    @Override
    public void stop() {}

    @Override
    public void start(final Infrastructure infra) {}

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener listener) {}

    @Override
    public boolean doesMessageKeyBelongToNode(final Object messageKey) {
        return true;
    }
}
