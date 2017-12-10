package net.dempsy.transport;

import net.dempsy.ServiceManager;

/**
 * Type specific {@link ServiceManager}
 */
public class TransportManager extends ServiceManager<SenderFactory> {
    public TransportManager() {
        super(SenderFactory.class);
    }
}
