package net.dempsy.router;

import static net.dempsy.util.Functional.uncheck;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;



import org.slf4j.Logger;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.DisruptibleSession;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.cluster.zookeeper.ZookeeperSession;
import net.dempsy.cluster.zookeeper.ZookeeperSessionFactory;
import net.dempsy.cluster.zookeeper.ZookeeperTestServer;
import net.dempsy.config.ClusterId;
import net.dempsy.serialization.jackson.JsonSerializer;
import net.dempsy.util.TestInfrastructure;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public abstract class BaseRouterTestWithSession {
    protected Logger LOGGER;

    protected Infrastructure infra = null;
    protected ClusterInfoSession session = null;
    protected AutoDisposeSingleThreadScheduler sched = null;

    protected Consumer<ClusterInfoSession> disruptor;
    protected ClusterInfoSessionFactory sessFact;

    protected String testName = null;

    protected ClusterId setTestName(final String testName) {
        final ClusterId cid = clusterId(testName);
        this.testName = cid.applicationName;
        return cid;
    }

    protected static ZookeeperTestServer zookeeperTestServer = null;
    protected static ClusterInfoSessionFactory zookeeperFactory = null;

    @SuppressWarnings("unchecked")
    protected void initParams(final Supplier<ClusterInfoSessionFactory> factorySupplier, final String disruptorName,
        final Consumer<ClusterInfoSession> disruptor) throws ClusterInfoException, IOException {
        final ClusterInfoSessionFactory factory = factorySupplier.get();
        LOGGER.debug("Running {}", factory.getClass().getSimpleName());
        this.sessFact = factory;
        this.disruptor = disruptor;
        setup();
    }

    public static Stream<Arguments> data() {
        return Arrays.<Object[]>asList(new Object[][] {
            {(Supplier<ClusterInfoSessionFactory>)() -> new LocalClusterSessionFactory(),"standard",
                (Consumer<ClusterInfoSession>)s -> ((DisruptibleSession)s).disrupt()},
            {(Supplier<ClusterInfoSessionFactory>)() -> zookeeperFactory,"standard",
                (Consumer<ClusterInfoSession>)s -> ((DisruptibleSession)s).disrupt()},
            {(Supplier<ClusterInfoSessionFactory>)() -> zookeeperFactory,"session-expire",
                (Consumer<ClusterInfoSession>)s -> uncheck(() -> zookeeperTestServer.forceSessionExpiration((ZookeeperSession)s))},
        }).stream().map(arr -> Arguments.of(arr));
    }

    @BeforeAll
    public static void setupClass() {
        zookeeperFactory = new ClusterInfoSessionFactory() {
            ZookeeperSessionFactory proxied = null;

            @Override
            public ClusterInfoSession createSession() throws ClusterInfoException {
                if(zookeeperTestServer == null)
                    zookeeperTestServer = uncheck(() -> new ZookeeperTestServer(2183));
                if(proxied == null)
                    proxied = new ZookeeperSessionFactory(zookeeperTestServer.connectString(), 5000, new JsonSerializer());
                return proxied.createSession();
            }

            @Override
            public String toString() {
                return "TestMicroshardingRoutingStrategy-Proxied-ZookeeperSessionFactory";
            }
        };

    }

    @AfterAll
    public static void teardown() {
        if(zookeeperTestServer != null) {
            zookeeperTestServer.close();
            zookeeperTestServer = null;
        }

        if(zookeeperFactory != null) {
            zookeeperFactory = null;
        }
    }

    protected void setup() throws ClusterInfoException, IOException {
        session = sessFact.createSession();
        sched = new AutoDisposeSingleThreadScheduler(testName + "-AutoDisposeSingleThreadScheduler");
        infra = makeInfra(session, sched);
    }

    @AfterEach
    public void after() {
        if(session != null)
            session.close();
        LocalClusterSessionFactory.completeReset();
    }

    public Infrastructure makeInfra(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        return new TestInfrastructure(testName == null ? "application" : testName, session, sched);
    }

    private static AtomicLong cidSequence = new AtomicLong(0L);

    public static ClusterId clusterId(final String testName) {
        return new ClusterId(testName + "-" + cidSequence.getAndIncrement(), "cluster");
    }
}
