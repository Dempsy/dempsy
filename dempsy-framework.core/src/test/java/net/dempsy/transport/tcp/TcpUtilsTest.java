package net.dempsy.transport.tcp;

import static net.dempsy.util.Functional.uncheck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class TcpUtilsTest {

    @Test
    public void testGetFirstNonLocalhostInetAddress() throws Exception {
        final InetAddress addr = TcpUtils.getFirstNonLocalhostInetAddress();
        assertNotNull(addr);
        assertFalse(addr.isLoopbackAddress());
    }

    @Test
    public void testGetFirstNonLocalhostInetAddressNamed() throws Exception {
        final List<NetworkInterface> ifs = Collections.list(TcpUtils.getInterfaces(null)).stream()
                .filter(nif -> !uncheck(() -> nif.isLoopback()))
                .collect(Collectors.toList());

        final NetworkInterface nif;
        final NetworkInterface notNif;

        if (ifs.size() > 1) {
            nif = ifs.get(1);
            notNif = ifs.get(0);
        } else if (ifs.size() == 1) {
            nif = ifs.get(0);
            notNif = null;
        } else {
            nif = null;
            notNif = null;
        }

        if (nif != null) { // otherwise we can do no testing.
            final String name = nif.getDisplayName();
            final List<InetAddress> expectedAddrs = Collections.list(nif.getInetAddresses());
            if (expectedAddrs.size() > 0) { // otherwise, we still can't really do anything without a lot of work
                final InetAddress expected = expectedAddrs.get(0);
                final InetAddress addr = TcpUtils.getFirstNonLocalhostInetAddress(name);
                assertEquals(expected, addr);

                if (notNif != null) {
                    final List<InetAddress> noneOfThese = Collections.list(notNif.getInetAddresses());
                    assertFalse(noneOfThese.contains(addr));
                }
            } else
                System.out.println("Can't test TcpUtils.getFirstNonLocalhostInetAddress(name)");
        } else
            System.out.println("Can't test TcpUtils.getFirstNonLocalhostInetAddress(name)");
    }
}
