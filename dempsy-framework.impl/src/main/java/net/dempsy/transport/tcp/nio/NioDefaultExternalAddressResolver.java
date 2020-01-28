package net.dempsy.transport.tcp.nio;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import net.dempsy.DempsyException;
import net.dempsy.transport.tcp.TcpAddressResolver;
import net.dempsy.transport.tcp.TcpUtils;

/**
 * <p>
 * This is used to convert the internal NioAddress to the external NioAddress that
 * will be registered with the cluster manager as the address of this node. This
 * is "docker aware" in that if you set the environment variable DOCKER_HOST
 * to the IP address of the host machine then it will use that IP address.
 * </p>
 *
 * <p>
 * DOCKER_HOST can be set to the IP address and port or just the port. To set it to
 * the IP address and port, separate them with a colon. If you want to just set the
 * port, the value should start with the colon. If there's no colon the value is
 * assumed to be an IP address.
 * </p>
 *
 */
public class NioDefaultExternalAddressResolver implements TcpAddressResolver<NioAddress> {
    public static final String DOCKER_HOST_ENV_VARIABLE = "DOCKER_HOST";

    @Override
    public NioAddress getExternalAddresses(final NioAddress addr) throws DempsyException {
        if(addr == null)
            throw new NullPointerException("Null address passed to resolver");
        final String dockerHost = System.getenv(DOCKER_HOST_ENV_VARIABLE);

        // If we set the environment variable DOCKER_HOST
        if(dockerHost != null) {
            final int colonIndex = dockerHost.indexOf(':');
            final String[] splitStr = colonIndex < 0 ? new String[] {dockerHost,null} : dockerHost.split(":", -1);
            final String ipAddr = (splitStr[0] == null || splitStr[0].trim().length() == 0) ? null : splitStr[0];
            final int port = splitStr[1] == null ? addr.port : Integer.parseInt(splitStr[1]);
            try {
                return new NioAddress(
                    ipAddr == null ? TcpUtils.getFirstNonLocalhostInetAddress() : InetAddress.getByName(ipAddr),
                    port, addr.serializerId, addr.recvBufferSize, addr.messageSizeLimit);
            } catch(final UnknownHostException | SocketException uhe) {
                throw new DempsyException(uhe, true);
            }
        }

        // Otherwise, if we bound the internal address to all interfaces
        if(addr.inetAddress == null || addr.inetAddress.isAnyLocalAddress()) {
            try {
                return new NioAddress(
                    TcpUtils.getFirstNonLocalhostInetAddress(),
                    addr.port, addr.serializerId, addr.recvBufferSize, addr.messageSizeLimit);
            } catch(final SocketException e) {
                throw new DempsyException(e, false);
            }
        }

        // otherwise, the external address is the internal address.
        return addr;
    }
}
