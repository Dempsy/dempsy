package net.dempsy.messages;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used to PULL the Dispatcher rather than have it pushed
 * to an adaptor. It can be injected into Dempsy and ALSO into another
 * class that can use it as a handle for the dispatcher.
 */
public class DispatcherAdaptor implements Adaptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherAdaptor.class);

    private Dispatcher dispatcher = null;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public synchronized void setDispatcher(final Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        running.set(false);
    }

    public synchronized boolean isReady() {
        return dispatcher != null && running.get();
    }

    public synchronized Dispatcher dipatcher() {
        return dispatcher;
    }

    public void dispatchAnnotated(final Object message)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException {
        if(running.get())
            dispatcher.dispatchAnnotated(message);
        else
            LOGGER.debug("dispatchAnnotated called on stopped " + DispatcherAdaptor.class.getSimpleName());
    }

    public void dispatchAnnotated(final Object message, final MessageResourceManager dispose)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException {
        if(running.get())
            dispatcher.dispatchAnnotated(message, dispose);
        else
            LOGGER.debug("dispatchAnnotated called on stopped " + DispatcherAdaptor.class.getSimpleName());
    }
}
