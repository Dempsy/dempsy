package net.dempsy.container.nonlocking;

import net.dempsy.Locator;

public class Factory implements Locator {

    @SuppressWarnings("deprecation")
    @Override
    public <T> T locate(final Class<T> clazz) {
        throw new UnsupportedOperationException("The " + NonLockingContainer.class.getSimpleName() + " is broken");
        // if(Container.class.equals(clazz))
        // return (T)new NonLockingContainer();
        // return null;
    }

}
