package net.dempsy.transport;

import net.dempsy.ServiceManager;

public class TransportManager extends ServiceManager<SenderFactory> {
    public TransportManager() {
        super(SenderFactory.class);
    }
}
