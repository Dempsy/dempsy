package net.dempsy.transport.tcp;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import net.dempsy.DempsyException;

public class TcpUtils {

    public static InetAddress getFirstNonLocalhostInetAddress() throws SocketException {
        final List<InetAddress> addrs = getAllInetAddress();
        return addrs.stream()
                .filter(i -> !i.isLoopbackAddress() && i instanceof Inet4Address)
                .findFirst()
                .orElseThrow(() -> new DempsyException("There are no non-local network interfaces among " + addrs));
    }

    public static List<InetAddress> getAllInetAddress() throws SocketException {
        final List<InetAddress> ret = new ArrayList<>();
        final Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = netInterfaces.nextElement();
            for (final Enumeration<InetAddress> loopInetAddress = networkInterface.getInetAddresses(); loopInetAddress.hasMoreElements();) {
                final InetAddress tempInetAddress = loopInetAddress.nextElement();
                ret.add(tempInetAddress);
            }
        }
        return ret;
    }

}
