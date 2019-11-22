package net.dempsy.messages;

import net.dempsy.DempsyException;

public class DummyDispatcher extends Dispatcher {

    @Override
    public void dispatch(final KeyedMessageWithType message, final MessageResourceManager dispose) throws DempsyException {
        // no where to go.
    }

}
