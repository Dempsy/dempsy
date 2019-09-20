package net.dempsy.test.manager.impl1;

import net.dempsy.Infrastructure;
import net.dempsy.test.manager.interf.SomeInterface;

public class SomeImpl implements SomeInterface {

    @Override
    public String callMe(final String calledWith) {
        return null;
    }

    @Override
    public void start(final Infrastructure infra) {}

    @Override
    public void stop() {}

    @Override
    public boolean isReady() {
        return false;
    }

}
