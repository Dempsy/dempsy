package net.dempsy.test.manager.impl2;

import net.dempsy.Locator;
import net.dempsy.test.manager.interf.SomeInterface;

public class Factory implements Locator {

    public static boolean locatorCalled = false;

    @SuppressWarnings("unchecked")
    @Override
    public <T> T locate(final Class<T> clazz) {
        locatorCalled = true;
        if(clazz.equals(SomeInterface.class))
            return (T)new SomeImpl();
        else
            return null;
    }

}
