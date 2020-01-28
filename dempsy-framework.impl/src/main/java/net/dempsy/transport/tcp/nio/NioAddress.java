package net.dempsy.transport.tcp.nio;

import java.net.InetAddress;

import net.dempsy.transport.tcp.TcpAddress;

public class NioAddress extends TcpAddress {
    private static final long serialVersionUID = 1L;

    public NioAddress(final InetAddress inetAddress, final int port, final String serializerId, final int recvBufferSize, final long messageSizeLimit) {
        super(inetAddress, port, serializerId, recvBufferSize, messageSizeLimit);
    }

    @SuppressWarnings("unused")
    private NioAddress() {}

}
