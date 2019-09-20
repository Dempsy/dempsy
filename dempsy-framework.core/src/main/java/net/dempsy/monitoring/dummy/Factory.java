package net.dempsy.monitoring.dummy;

import net.dempsy.Locator;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;

public class Factory implements Locator {

    @SuppressWarnings("unchecked")
    @Override
    public <T> T locate(final Class<T> clazz) {
        if(ClusterStatsCollectorFactory.class.equals(clazz))
            return (T)new DummyStatsCollectorFactory();
        return null;
    }

}
