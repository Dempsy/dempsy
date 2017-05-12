package net.dempsy.router.group;

import static net.dempsy.router.group.intern.GroupUtils.groupNameFromTypeIdDontThrowNoColon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Router;
import net.dempsy.util.SafeString;

public class ClusterGroupRouterFactory implements RoutingStrategy.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterGroupRouterFactory.class);

    private final Map<ClusterId, ClusterGroupRouter> cache = new HashMap<>();
    private Infrastructure infra = null;
    private String groupName;
    private String thisNode;

    @Override
    public void start(final Infrastructure infra) {
        this.infra = infra;
        this.thisNode = infra.getNodeId();
    }

    @Override
    public void typeId(final String typeId) {
        groupName = groupNameFromTypeIdDontThrowNoColon(typeId);
    }

    @Override
    public synchronized void stop() {
        final List<ClusterGroupRouter> tmp = new ArrayList<>(cache.values());
        tmp.forEach(s -> {
            try {
                s.release();
            } catch (final RuntimeException rte) {
                LOGGER.error("Failure shutting down routing strategy", rte);
            }
        });
        if (!cache.isEmpty())
            throw new IllegalStateException("What happened?");
    }

    @Override
    public synchronized Router getStrategy(final ClusterId clusterId) {
        ClusterGroupRouter ret = cache.get(clusterId);
        if (ret == null) {
            if (groupName == null)
                LOGGER.warn("No group specified for cluster group inbound for " + clusterId + " at " + thisNode
                        + " using the cluster name. You should choose a different routing strategy.");
            ret = new ClusterGroupRouter(this, clusterId, infra, groupName == null ? clusterId.clusterName : groupName);
            cache.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public boolean isReady() {
        for (final ClusterGroupRouter r : cache.values()) {
            if (!r.isReady())
                return false;
        }
        return true;
    }

    synchronized void release(final Router strategy) {
        if (!ClusterGroupRouter.class.isAssignableFrom(strategy.getClass()))
            throw new IllegalArgumentException("Can't relase " + SafeString.objectDescription(strategy) + " because it's not the correct type.");
        final ClusterGroupRouter it = (ClusterGroupRouter) strategy;
        synchronized (this) {
            final ClusterGroupRouter whatIHave = cache.remove(it.clusterId);
            if (whatIHave == null || it != whatIHave)
                throw new IllegalArgumentException("Can't release " + SafeString.objectDescription(strategy) + " because I'm not managing it.");
        }
    }

}
