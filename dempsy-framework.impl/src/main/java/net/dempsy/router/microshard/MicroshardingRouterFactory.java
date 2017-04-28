package net.dempsy.router.microshard;

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

public class MicroshardingRouterFactory implements RoutingStrategy.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroshardingRouterFactory.class);

    private final Map<ClusterId, MicroshardingRouter> cache = new HashMap<>();
    private Infrastructure infra = null;

    @Override
    public void start(final Infrastructure infra) {
        this.infra = infra;
    }

    @Override
    public synchronized void stop() {
        final List<MicroshardingRouter> tmp = new ArrayList<>(cache.values());
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
        MicroshardingRouter ret = cache.get(clusterId);
        if (ret == null) {
            ret = new MicroshardingRouter(this, clusterId, infra);
            cache.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public boolean isReady() {
        for (final MicroshardingRouter r : cache.values()) {
            if (!r.isReady())
                return false;
        }
        return true;
    }

    synchronized void release(final Router strategy) {
        if (!MicroshardingRouter.class.isAssignableFrom(strategy.getClass()))
            throw new IllegalArgumentException("Can't relase " + SafeString.objectDescription(strategy) + " because it's not the correct type.");
        final MicroshardingRouter it = (MicroshardingRouter) strategy;
        synchronized (this) {
            final MicroshardingRouter whatIHave = cache.remove(it.clusterId);
            if (whatIHave == null || it != whatIHave)
                throw new IllegalArgumentException("Can't release " + SafeString.objectDescription(strategy) + " because I'm not managing it.");
        }
    }

}
