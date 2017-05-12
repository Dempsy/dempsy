package net.dempsy.router;

import net.dempsy.Manager;

/**
 * Routing strategy manager handles extended type information
 */
public class RoutingInboundManager extends Manager<RoutingStrategy.Inbound> {
    public RoutingInboundManager() {
        super(RoutingStrategy.Inbound.class);
    }

    @Override
    public RoutingStrategy.Inbound makeInstance(final String typeId) {
        final String[] split = typeId.split(":");
        final RoutingStrategy.Inbound ret = super.makeInstance(split[0]);
        ret.typeId(typeId);
        return ret;
    }
}
