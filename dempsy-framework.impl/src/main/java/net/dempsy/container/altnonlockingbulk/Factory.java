package net.dempsy.container.altnonlockingbulk;

import net.dempsy.Locator;
import net.dempsy.container.Container;

public class Factory implements Locator {

    @SuppressWarnings("unchecked")
    @Override
    public <T> T locate(final Class<T> clazz) {
        if(Container.class.equals(clazz))
            return (T)new NonLockingAltBulkContainer();
        return null;
    }

}
