package net.dempsy.router.simple;

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

public class SimpleRoutingStrategyFactory implements RoutingStrategy.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRoutingStrategyFactory.class);

    private final Map<ClusterId, SimpleRoutingStrategy> cache = new HashMap<>();
    private Infrastructure infra = null;

    @Override
    public void start(final Infrastructure infra) {
        this.infra = infra;
    }

    @Override
    public synchronized void stop() {
        final List<SimpleRoutingStrategy> tmp = new ArrayList<>(cache.values());
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
        SimpleRoutingStrategy ret = cache.get(clusterId);
        if (ret == null) {
            ret = new SimpleRoutingStrategy(this, clusterId, infra);
            cache.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public boolean isReady() {
        if (infra == null)
            return false;
        for (final SimpleRoutingStrategy s : cache.values()) {
            if (!s.isReady())
                return false;
        }
        return true;
    }

    void release(final Router strategy) {
        if (!SimpleRoutingStrategy.class.isAssignableFrom(strategy.getClass()))
            throw new IllegalArgumentException("Can't relase " + SafeString.objectDescription(strategy) + " because it's not the correct type.");
        final SimpleRoutingStrategy it = (SimpleRoutingStrategy) strategy;
        synchronized (this) {
            final SimpleRoutingStrategy whatIHave = cache.remove(it.clusterId);
            if (whatIHave == null || it != whatIHave)
                throw new IllegalArgumentException("Can't release " + SafeString.objectDescription(strategy) + " because I'm not managing it.");
        }
    }
}
