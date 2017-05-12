package net.dempsy.router;

import net.dempsy.ServiceManager;

/**
 * Routing strategy manager handles extended type information
 */
public class RoutingStrategyManager extends ServiceManager<RoutingStrategy.Factory> {
    public RoutingStrategyManager() {
        super(RoutingStrategy.Factory.class);
    }

    @Override
    public RoutingStrategy.Factory makeInstance(final String typeId) {
        final String[] split = typeId.split(":");
        final RoutingStrategy.Factory ret = super.makeInstance(split[0]);
        ret.typeId(typeId);
        return ret;
    }
}
