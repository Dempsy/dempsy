package net.dempsy.router;

import net.dempsy.ServiceManager;

public class RoutingStrategyManager extends ServiceManager<RoutingStrategy.Factory> {
    public RoutingStrategyManager() {
        super(RoutingStrategy.Factory.class);
    }
}
