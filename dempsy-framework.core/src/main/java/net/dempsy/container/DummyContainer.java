package net.dempsy.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessage;

public class DummyContainer extends Container {
    private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

    public DummyContainer() {
        super(LOGGER);
        setClusterId(new ClusterId("DummyContainerApp", "DummyContainerCluster"));
    }

    @Override
    public boolean isReady() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getProcessorCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMessageWorkingCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dispatch(final KeyedMessage message, final Operation op, final boolean youOwnMessage) throws IllegalArgumentException, ContainerException {
        // we don't do much
    }

    @Override
    protected void doevict(final EvictCheck check) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void outputPass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getMp(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containerInternallyQueuesMessages() {
        return false;
    }

    @Override
    public boolean containerSupportsBulkProcessing() {
        return false;
    }

    @Override
    public boolean containerIsThreadSafe() {
        return true;
    }
}
