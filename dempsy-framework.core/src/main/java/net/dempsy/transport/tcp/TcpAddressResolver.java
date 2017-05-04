package net.dempsy.transport.tcp;

import net.dempsy.DempsyException;
import net.dempsy.transport.Receiver;

/**
 * This class can be used during the instantiation of any Tcp based transport {@link Receiver} in order to
 * map internal ports bound to, to external ports.
 */
@FunctionalInterface
public interface TcpAddressResolver<T extends TcpAddress> {

    public T getExternalAddresses(final T addr) throws DempsyException;
}
