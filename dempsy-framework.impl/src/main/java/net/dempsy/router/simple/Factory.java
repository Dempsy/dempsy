package net.dempsy.router.simple;

import net.dempsy.Locator;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Inbound;

public class Factory implements Locator {

    @SuppressWarnings("unchecked")
    @Override
    public <T> T locate(final Class<T> clazz) {
        if(Inbound.class.equals(clazz))
            return (T)new SimpleInboundSide();
        else if(RoutingStrategy.Factory.class.equals(clazz))
            return (T)new SimpleRoutingStrategyFactory();
        return null;
    }

}
