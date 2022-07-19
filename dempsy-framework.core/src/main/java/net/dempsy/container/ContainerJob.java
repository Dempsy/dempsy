package net.dempsy.container;

import net.dempsy.container.Container.ContainerSpecific;
import net.dempsy.container.Container.Operation;
import net.dempsy.messages.KeyedMessage;

public abstract class ContainerJob {

    public final ContainerSpecific individuatedCs;

    protected ContainerJob(final ContainerSpecific cs) {
        individuatedCs = cs;
    }

    protected ContainerJob() {
        this(null);
    }

    public abstract void execute(Container container);

    public abstract void reject(Container container);

    protected void dispatch(final Container c, final KeyedMessage message, final Operation op, final boolean justArrived) {
        if(individuatedCs != null)
            c.dispatch(message, op, individuatedCs, justArrived);
        else
            c.dispatch(message, op, justArrived);
    }

    protected void reject(final Container container, final KeyedMessage message, final boolean justArrived) {
        if(individuatedCs != null)
            container.messageBeingRejectedExternally(message, justArrived, individuatedCs);
    }

}
