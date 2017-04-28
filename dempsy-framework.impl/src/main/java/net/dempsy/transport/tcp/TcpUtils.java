package net.dempsy.transport.tcp;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import net.dempsy.DempsyException;

public class TcpUtils {

    public static InetAddress getFirstNonLocalhostInetAddress() throws SocketException {
        final Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = netInterfaces.nextElement();
            for (final Enumeration<InetAddress> loopInetAddress = networkInterface.getInetAddresses(); loopInetAddress.hasMoreElements();) {
                final InetAddress tempInetAddress = loopInetAddress.nextElement();
                if (!tempInetAddress.isLoopbackAddress() && tempInetAddress instanceof Inet4Address)
                    return tempInetAddress;
            }
        }
        throw new DempsyException("There are no non-local network interfaces among " + Collections.list(netInterfaces));
    }

}
