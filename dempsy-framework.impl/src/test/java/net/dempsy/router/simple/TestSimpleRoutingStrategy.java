package net.dempsy.router.simple;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.transport.NodeAddress;
import net.dempsy.util.TestInfrastructure;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public class TestSimpleRoutingStrategy {
    Infrastructure infra = null;
    LocalClusterSessionFactory sessFact = null;
    ClusterInfoSession session = null;
    AutoDisposeSingleThreadScheduler sched = null;

    private static Infrastructure makeInfra(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        return new TestInfrastructure(session, sched);
    }

    @Before
    public void setup() {
        sessFact = new LocalClusterSessionFactory();
        session = sessFact.createSession();
        sched = new AutoDisposeSingleThreadScheduler("test");

        infra = makeInfra(session, sched);
    }

    @After
    public void after() {
        if (session != null)
            session.close();
    }

    @Test
    public void testInboundHappyPathRegister() throws Exception {
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName());) {

            assertNotNull(ib);
            assertTrue(SimpleInboundSide.class.isAssignableFrom(ib.getClass()));

            ib.setContainerDetails(new ClusterId("test", "test"), new ContainerAddress(new DummyNodeAddress(), 0), (l, m, i) -> {});
            ib.start(infra);

            assertTrue(waitForReg(session));
        }
    }

    @Test
    public void testInboundResillience() throws Exception {
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName());) {

            assertNotNull(ib);
            assertTrue(SimpleInboundSide.class.isAssignableFrom(ib.getClass()));

            ib.setContainerDetails(new ClusterId("test", "test"), new ContainerAddress(new DummyNodeAddress(), 0), (l, m, i) -> {});
            ib.start(infra);

            assertTrue(waitForReg(session));

            // get the current ephem dir
            final String actualDir = ((SimpleInboundSide) ib).getAddressSubdirectory();
            assertNotNull(actualDir);

            session.rmdir(actualDir);

            assertTrue(waitForReg(session));

            final String newDir = ((SimpleInboundSide) ib).getAddressSubdirectory();

            assertNotEquals(actualDir, newDir);
        }
    }

    @Test
    public void testInboundWithOutbound() throws Exception {
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName());) {

            assertNotNull(ib);
            assertTrue(SimpleInboundSide.class.isAssignableFrom(ib.getClass()));

            final ClusterId cid = new ClusterId("test", "test");
            ib.setContainerDetails(cid, new ContainerAddress(new DummyNodeAddress("here"), 0), (l, m, i) -> {});
            ib.start(infra);

            assertTrue(waitForReg(session));

            try (final ClusterInfoSession ses2 = sessFact.createSession()) {
                try (final RoutingStrategyManager obman = chain(new RoutingStrategyManager(), o -> o.start(makeInfra(ses2, sched)));
                        final RoutingStrategy.Factory obf = obman.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName());) {
                    obf.start(makeInfra(ses2, sched));
                    final RoutingStrategy.Router ob = obf.getStrategy(cid);

                    final KeyedMessageWithType km = new KeyedMessageWithType(null, null, "");
                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));

                    final ContainerAddress ca = ob.selectDestinationForMessage(km);
                    assertNotNull(ca);

                    assertEquals("here", ((DummyNodeAddress) ca.node).name);

                    // now distupt the session
                    session.close();

                    // the destination should clear until a new in runs
                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) == null));

                    try (ClusterInfoSession ses3 = sessFact.createSession();
                            RoutingStrategy.Inbound ib2 = manager.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName())) {
                        ib2.setContainerDetails(cid, ca, (l, m, i) -> {});
                        ib2.start(makeInfra(ses3, sched));

                        assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));
                    }
                }
            }
        }
    }

    private static class DummyNodeAddress implements NodeAddress {
        private static final long serialVersionUID = 1L;
        public final String name;

        public DummyNodeAddress(final String name) {
            this.name = name;
        }

        public DummyNodeAddress() {
            this("yo");
        }
    }

    private static boolean waitForReg(final ClusterInfoSession session) throws InterruptedException {
        return poll(o -> {
            try {
                final Collection<String> subdirs = session.getSubdirs("/application/clusters/test", null);
                return subdirs.size() == 1;
            } catch (final ClusterInfoException e) {
                return false;
            }
        });
    }
}
