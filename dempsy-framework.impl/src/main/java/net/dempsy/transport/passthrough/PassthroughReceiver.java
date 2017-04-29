package net.dempsy.transport.passthrough;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;

public class PassthroughReceiver implements Receiver {
    final static Map<Long, PassthroughReceiver> receivers = new HashMap<>();
    private final static AtomicLong destinationIdSequence = new AtomicLong(0);

    private final PassthroughAddress destination = new PassthroughAddress(destinationIdSequence.getAndIncrement());
    Listener<Object> listener = null;

    public PassthroughReceiver() {
        synchronized (receivers) {
            receivers.put(destination.destinationId, this);
        }
    }

    @Override
    public NodeAddress getAddress() throws MessageTransportException {
        return destination;
    }

    /**
     * A receiver is started with a Listener and a threading model.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void start(final Listener<?> listener, final ThreadingModel threadingModel) throws MessageTransportException {
        this.listener = (Listener<Object>) listener;
    }

    @Override
    public void close() throws Exception {
        synchronized (receivers) {
            receivers.remove(destination.destinationId);
        }
    }
}
