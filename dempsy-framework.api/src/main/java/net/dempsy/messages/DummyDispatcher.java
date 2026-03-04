package net.dempsy.messages;

import net.dempsy.DempsyException;

/**
 * No-op {@link Dispatcher} implementation where dispatched messages are silently discarded.
 * Useful as a placeholder when no downstream routing is configured.
 */
public class DummyDispatcher extends Dispatcher {

    @Override
    public void dispatch(final KeyedMessageWithType message, final MessageResourceManager dispose) throws DempsyException {
        // no where to go.
    }

}
