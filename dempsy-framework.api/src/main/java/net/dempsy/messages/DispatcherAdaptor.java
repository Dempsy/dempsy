package net.dempsy.messages;

import java.util.concurrent.atomic.AtomicBoolean;

import net.dempsy.DempsyException;

/**
 * This class can be used to PULL the Dispatcher rather than have it pushed
 * to an adaptor. It can be injected into Dempsy and ALSO into another
 * class that can use it as a handle for the dispatcher.
 */
public class DispatcherAdaptor extends Dispatcher implements Adaptor {
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

    @Override
    public void dispatch(final KeyedMessageWithType message) throws DempsyException {
        dispatcher.dispatch(message);
    }

    public synchronized boolean isReady() {
        return dispatcher != null && running.get();
    }

    public synchronized Dispatcher getUnderlying() {
        return dispatcher;
    }
}
