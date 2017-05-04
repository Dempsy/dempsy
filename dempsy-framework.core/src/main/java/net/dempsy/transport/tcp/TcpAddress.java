package net.dempsy.transport.tcp;

import java.net.InetAddress;

import net.dempsy.transport.NodeAddress;

public abstract class TcpAddress implements NodeAddress {

    private static final long serialVersionUID = 1L;

    public final String guid;
    public final InetAddress inetAddress;
    public final int port;
    public final String serializerId;
    public final int recvBufferSize;

    protected TcpAddress() {
        guid = null;
        inetAddress = null;
        port = -1;
        serializerId = null;
        recvBufferSize = -1;
    }

    public TcpAddress(final InetAddress inetAddress, final int port, final String serializerId, final int recvBufferSize) {
        this.inetAddress = inetAddress;
        this.port = port;
        this.guid = inetAddress.getHostAddress() + ":" + port;
        this.serializerId = serializerId;
        this.recvBufferSize = recvBufferSize;
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
