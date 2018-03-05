package net.dempsy.transport.tcp;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import net.dempsy.DempsyException;

public class TcpUtils {

    public static InetAddress getFirstNonLocalhostInetAddress() throws SocketException {
        return getFirstNonLocalhostInetAddress(null);
    }

    public static InetAddress getFirstNonLocalhostInetAddress(final String interfaceName) throws SocketException {
        final List<InetAddress> addrs = getAllInetAddress(interfaceName);
        return addrs.stream()
                .filter(i -> !i.isLoopbackAddress() && i instanceof Inet4Address)
                .findFirst()
                .orElseThrow(() -> new DempsyException("There are no non-local network interfaces among " + addrs));
    }

    public static List<InetAddress> getAllInetAddress() throws SocketException {
        return getAllInetAddress(null);
    }

    public static List<InetAddress> getAllInetAddress(final String interfaceName) throws SocketException {
        final List<InetAddress> ret = new ArrayList<>();
        final Enumeration<NetworkInterface> netInterfaces = getInterfaces(interfaceName);
        while (netInterfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = netInterfaces.nextElement();
            for (final Enumeration<InetAddress> loopInetAddress = networkInterface.getInetAddresses(); loopInetAddress.hasMoreElements();) {
                final InetAddress tempInetAddress = loopInetAddress.nextElement();
                ret.add(tempInetAddress);
            }
        }
        return ret;
    }

    static Enumeration<NetworkInterface> getInterfaces(final String interfaceName) throws SocketException {
        return interfaceName != null
                ? new Vector<NetworkInterface>(Arrays.asList(NetworkInterface.getByName(interfaceName))).elements()
                : NetworkInterface.getNetworkInterfaces();

    }
}
