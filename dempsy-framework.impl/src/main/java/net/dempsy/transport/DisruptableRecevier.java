package net.dempsy.transport;

/**
 * This interface is implemented by {@link Receiver}s that should go through tests where the
 * connectivity is disrupted to make sure that the related {@link Sender} recovers. 
 */
public interface DisruptableRecevier {

    public boolean disrupt(NodeAddress senderToDistrupt);

}
