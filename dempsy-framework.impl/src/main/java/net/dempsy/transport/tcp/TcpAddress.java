package net.dempsy.transport.tcp;

import java.net.InetAddress;

import net.dempsy.transport.NodeAddress;

public class TcpAddress implements NodeAddress {

    private static final long serialVersionUID = 1L;

    public final String guid;
    public final InetAddress inetAddress;
    public final int port;
    public final String serializerId;

    @SuppressWarnings("unused")
    private TcpAddress() {
        guid = null;
        inetAddress = null;
        port = -1;
        serializerId = null;
    }

    public TcpAddress(final InetAddress inetAddress, final int port, final String serializerId) {
        this.inetAddress = inetAddress;
        this.port = port;
        this.guid = inetAddress.getHostAddress() + ":" + port;
        this.serializerId = serializerId;
    }

    @Override
    public String getGuid() {
        return guid;
    }

    @Override
    public String toString() {
        return guid;
    }

    @Override
    public int hashCode() {
        return guid.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TcpAddress other = (TcpAddress) obj;
        if (guid == null) {
            if (other.guid != null)
                return false;
        } else if (!guid.equals(other.guid))
            return false;
        return true;
    }

}
