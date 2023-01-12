package net.dempsy;

import static net.dempsy.util.Functional.ignore;
import static net.dempsy.util.Functional.reverseRange;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static net.dempsy.utils.test.ConditionPoll.qpoll;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.serialization.util.ClassTracker;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.threading.OrderedPerContainerThreadingModel;
import net.dempsy.threading.OrderedPerContainerThreadingModelAlt;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.blockingqueue.BlockingQueueAddress;
import net.dempsy.util.SystemPropertyManager;
import net.dempsy.utils.test.CloseableRule;

@RunWith(Parameterized.class)
public abstract class DempsyBaseTest {

    public static final long TEN_SECONDS = 10000;

    @ClassRule public static CloseableRule props = new CloseableRule(new SystemPropertyManager().set("org.quartz.threadPool.threadCount", "1"));

    /**
     * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in
     * every runCombos call. This can make tests run for a loooooong time.
     */
    public static boolean hardcore = Boolean.parseBoolean(System.getProperty("hardcore", "false"));

    /**
     * If this is set to <code>true</code> then the serializers will be rotated through but
     * not every combination of serializer with the other possibilities will be tried. This is
     * because the serializer testing is totally orthogonal to everything else and so setting
     * this to false isn't likely to provide any better results than running the test multiple
     * times.
     */
    public static boolean butRotateSerializer = Boolean.parseBoolean(System.getProperty("butRotateSerializer", "true"));

    protected Logger LOGGER;

    /**
     * Contains a generic node specification. It needs to following variables set:
     *
     * <ul>
     * <li>routing-strategy - set the to the routing strategy id.</li>
     * <li>container-type - set to the container type id</li>
     * <li>routing-group</li> - can be set for group strategies. Shouldn't be set otherwise. Defaults to "group."
     * <li>min_nodes</li> - for managed and group strategies this sets the min_nodes to expect. Defaults to "1."
     * <li>total_shards</li> - for managed and group strategies this sets the total_shards. It must be a power of "2."
     * It's currently defaults to {@link DempsyBaseTest#NUM_MICROSHARDS} in the {@link DempsyBaseTest#runCombos}
     * methods.
     * </ul>
     *
     * <p>
     * It autowires all of the {@link Cluster}'s that appear in the same context.
     * </p>
     *
     * <p>
     * Currently it directly uses the {@code BasicStatsCollector}
     * </p>
     */
    protected static String nodeCtx = "classpath:/td/node.xml";

    protected final String routerId;
    protected final String containerId;
    protected final String sessionType;
    protected final String transportType;
    protected final String serializerType;
    protected final String threadingModelDescription;
    protected final Function<String, ThreadingModel> threadingModelSource;

    protected static final String ROUTER_ID_PREFIX = "net.dempsy.router.";
    protected static final String CONTAINER_ID_PREFIX = "net.dempsy.container.";
    protected static final String COLLAB_CTX_PREFIX = "classpath:/td/collab-";
    protected static final String COLLAB_CTX_SUFFIX = ".xml";
    protected static final String TRANSPORT_CTX_PREFIX = "classpath:/td/transport-";
    protected static final String TRANSPORT_CTX_SUFFIX = ".xml";
    protected static final String SERIALIZER_CTX_PREFIX = "classpath:/td/serializer-";
    protected static final String SERIALIZER_CTX_SUFFIX = ".xml";
    protected static final int NUM_MICROSHARDS = 16;

    protected DempsyBaseTest(final Logger logger, final String routerId, final String containerId, final String sessionType,
        final String transportType, final String serializerType, final String threadingModelDescription,
        final Function<String, ThreadingModel> threadingModelSource) {
        this.LOGGER = logger;
        this.routerId = routerId;
        this.containerId = containerId;
        this.sessionType = sessionType;
        this.transportType = transportType;
        this.serializerType = serializerType;
        this.threadingModelSource = threadingModelSource;
        this.threadingModelDescription = threadingModelDescription;
    }

    private static class Combos {
        final String[] routers;
        final String[] containers;
        final String[] sessions;
        final String[] transports;
        final String[] serializers;
        final Object[][] threadingDetails;

        public Combos(final String[] routers, final String[] containers, final String[] sessions, final String[] transports,
            final String[] serializers, final Object[][] threadingDetails) {
            this.routers = routers;
            this.containers = containers;
            this.sessions = sessions;
            this.transports = transports;
            this.serializers = serializers;
            this.threadingDetails = threadingDetails;
        }
    }

    private static List<String> elasticRouterIds = Arrays.asList("managed", "group");
    private static List<String> transportsThatRequireSerializer = Arrays.asList("nio");
    private static List<String> groupRoutingStrategies = Arrays.asList("group");
    private static List<String> containersThatSupportLimitedQueueLen = Arrays.asList("locking");
    private static List<String> containersThatDontInternallyQueue = Arrays.asList("locking", "simple");
    private static List<String> containersThatArentThreadSafe = Arrays.asList("simple");

    // ======================================================
    // These define how the tests use the threading model. Don't
    // forget that there's a separate DefaultThreadingModel for
    // each node.
    // ======================================================
    public static final double TM_CORES_FACTOR = 0.2;
    public static final int TM_ADDITIONAL_THREADS = 2;
    public static final long TM_QUEUE_DEPTH_WHEN_LIMITED = 100000L;
    // ======================================================

    public static Combos hardcore() {
        return new Combos(
            new String[] {"simple","managed","group"},
            // the non-locking container is broken
            new String[] {"locking","altnonlocking","altnonlockingbulk"},
            new String[] {"local","zookeeper"},
            new String[] {"bq","passthrough","nio"},
            new String[] {"json","java","kryo"},
            new Object[][] {
                {"blocking",(Function<String, ThreadingModel>)(testName) -> new DefaultThreadingModel(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setBlocking(true)
                    .setMaxNumberOfQueuedLimitedTasks(TM_QUEUE_DEPTH_WHEN_LIMITED)
                },
                // limited = max len set and blocking = false. That means messages will be thrown away.
                // This could also be called nonblocking.
                {"limited",(Function<String, ThreadingModel>)(testName) -> new DefaultThreadingModel(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setBlocking(false)
                    .setMaxNumberOfQueuedLimitedTasks(TM_QUEUE_DEPTH_WHEN_LIMITED)
                },
                {"unbounded",(Function<String, ThreadingModel>)(testName) -> new DefaultThreadingModel(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setBlocking(false)
                    .setMaxNumberOfQueuedLimitedTasks(-1)
                },
                {"ordered",(Function<String, ThreadingModel>)(testName) -> new OrderedPerContainerThreadingModel(testName)
                    .setMaxNumberOfQueuedLimitedTasks(TM_QUEUE_DEPTH_WHEN_LIMITED)
                },
                {"orderedAlt",(Function<String, ThreadingModel>)(testName) -> new OrderedPerContainerThreadingModelAlt(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setMaxNumberOfQueuedLimitedTasks(-1)
                },
            });
    }

    public static Combos production() {
        return new Combos(
            new String[] {"managed","group"},
            new String[] {"simple","altnonlockingbulk","locking"},
            new String[] {"zookeeper"},
            new String[] {"nio","bq"},
            new String[] {"kryo"},
            new Object[][] {
                // limited = max len set and blocking = false. That means messages will be thrown away.
                // This could also be called nonblocking.
                {"limited",(Function<String, ThreadingModel>)(testName) -> new DefaultThreadingModel(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setBlocking(false)
                    .setMaxNumberOfQueuedLimitedTasks(TM_QUEUE_DEPTH_WHEN_LIMITED)
                },
                {"ordered",(Function<String, ThreadingModel>)(testName) -> new OrderedPerContainerThreadingModel(testName)
                    .setMaxNumberOfQueuedLimitedTasks(TM_QUEUE_DEPTH_WHEN_LIMITED)
                },
                {"orderedAlt",(Function<String, ThreadingModel>)(testName) -> new OrderedPerContainerThreadingModelAlt(testName)
                    .setAdditionalThreads(TM_ADDITIONAL_THREADS)
                    .setCoresFactor(TM_CORES_FACTOR)
                    .setMaxNumberOfQueuedLimitedTasks(-1)
                },
            });
    }

    public static boolean requiresSerialization(final String transport) {
        return transportsThatRequireSerializer.contains(transport);
    }

    public static boolean isGroupRoutingStrategy(final String routerId) {
        return groupRoutingStrategies.contains(routerId);
    }

    public static boolean isElasticRoutingStrategy(final String routerId) {
        return elasticRouterIds.contains(routerId);
    }

    public static boolean containerSupportsLimitedQueueLen(final String containerId) {
        return containersThatSupportLimitedQueueLen.contains(containerId);
    }

    public static boolean containerDoesntInternallyQueue(final String containerId) {
        return containersThatDontInternallyQueue.contains(containerId);
    }

    public static boolean containerInternallyQueues(final String containerId) {
        return !containerDoesntInternallyQueue(containerId);
    }

    public static boolean containerNotThreadSafe(final String containerId) {
        return containersThatArentThreadSafe.contains(containerId);
    }

    @Parameters(name = "{index}: routerId={0}, container={1}, cluster={2}, threading={5}, transport={3}/{4}")
    public static Collection<Object[]> combos() {
        final Combos combos = (hardcore) ? hardcore() : production();

        // since serialization is orthogonal to the transport we wont test every transport
        // with every serializer. Instead we'll rotate the serializers if that's requested.
        int serializerIndex = 0;

        final List<Object[]> ret = new ArrayList<>();
        for(final String router: combos.routers) {
            for(final String container: combos.containers) {
                for(final String sessFact: combos.sessions) {
                    for(final Object[] threading: combos.threadingDetails) {
                        // OrderedPerContainerThreadingModel can't be used with
                        // containers that internally queue.
                        if(threading[0].toString().startsWith("ordered") && containerInternallyQueues(container))
                            continue;

                        // if the container is not thread safe, then it's incompatible with anything but ordered threading models.
                        if(containerNotThreadSafe(container) && !threading[0].toString().startsWith("ordered"))
                            continue;

                        for(final String tp: combos.transports) {
                            if(requiresSerialization(tp)) {
                                if(butRotateSerializer) {
                                    ret.add(new Object[] {router,container,sessFact,tp,
                                        combos.serializers[(serializerIndex % combos.serializers.length)],
                                        threading[0],threading[1]});
                                    serializerIndex++;
                                } else {
                                    for(final String ser: combos.serializers)
                                        ret.add(new Object[] {router,container,sessFact,tp,ser,threading[0],threading[1]});
                                }
                            } else
                                ret.add(new Object[] {router,container,sessFact,tp,"none",threading[0],threading[1]});
                        }
                    }
                }
            }
        }
        return ret;
    }

    public static class NodeManagerWithContext implements AutoCloseable {
        public final NodeManager manager;
        public final ClassPathXmlApplicationContext ctx;

        public NodeManagerWithContext(final NodeManager manager, final ClassPathXmlApplicationContext ctx) {
            this.manager = manager;
            this.ctx = ctx;
        }

        @Override
        public void close() throws Exception {
            manager.close();
            ctx.close();
        }
    }

    public static class Nodes {
        public final List<NodeManagerWithContext> nodes;
        public final ClusterInfoSessionFactory sessionFactory;

        public Nodes(final List<NodeManagerWithContext> nodes, final ClusterInfoSessionFactory sessionFactory) {
            this.nodes = nodes;
            this.sessionFactory = sessionFactory;
        }
    }

    @FunctionalInterface
    public static interface TestToRun {
        public void test(Nodes nodes) throws Exception;

        public default void postShutdown() throws Exception {}
    }

    @FunctionalInterface
    public static interface ComboFilter {
        public boolean filter(final String routerId, final String containerId, final String sessionType, final String transportId,
            final String serializerType);
    }

    private static final String[] frameworkCtx = {nodeCtx};

    // ==============================================================================
    // Test state information accessible to the subclasses.
    // ==============================================================================
    protected ServiceTracker currentlyTracking = null;
    protected ClusterInfoSessionFactory currentSessionFactory = null;
    protected String currentAppName = null;
    // ==============================================================================

    protected void stopSystem() throws Exception {
        currentSessionFactory = null;
        if(currentlyTracking != null)
            currentlyTracking.close();
        else
            throw new IllegalStateException("Not currently tracking any Dempsy system");

    }

    protected void runCombos(final String testName, final String[][] ctxs, final TestToRun test) throws Exception {
        runCombos(testName, null, ctxs, null, test);
    }

    private static AtomicLong runComboSequence = new AtomicLong(0);

    protected void runCombos(final String testName, final ComboFilter filter, final String[][] ctxs, final TestToRun test) throws Exception {
        runCombos(testName, filter, ctxs, null, test);
    }

    protected void runCombos(final String testName, final ComboFilter filter, final String[][] ctxs, final String[][][] perNodeProps,
        final TestToRun test) throws Exception {
        if(filter != null && !filter.filter(routerId, containerId, sessionType, transportType, serializerType))
            return;

        final long comboSequence = runComboSequence.getAndIncrement();
        currentAppName = testName + "-" + comboSequence;

        LOGGER.info("=====================================================================================");
        LOGGER.info("======== Running (" + comboSequence + ") " + testName + " with " + routerId + ", " + containerId + ", " + sessionType + ", "
            + threadingModelDescription + ", " + transportType + "/" + serializerType);

        try(final ServiceTracker tr = new ServiceTracker()) {
            currentlyTracking = tr;
            tr.track(new SystemPropertyManager())
                .set("routing-strategy", ROUTER_ID_PREFIX + routerId)
                .set("container-type", CONTAINER_ID_PREFIX + containerId)
                .set("test-name", currentAppName)
                .setIfAbsent("total_shards", Integer.toString(NUM_MICROSHARDS));

            // instantiate session factory
            final ClusterInfoSessionFactory sessFact = tr
                .track(new ClassPathXmlApplicationContext(COLLAB_CTX_PREFIX + sessionType + COLLAB_CTX_SUFFIX))
                .getBean(ClusterInfoSessionFactory.class);

            currentSessionFactory = sessFact;

            final List<NodeManagerWithContext> reverseCpCtxs = reverseRange(0, ctxs.length)
                .mapToObj(i -> {
                    try(final SystemPropertyManager p2 = new SystemPropertyManager()) {
                        if(perNodeProps != null && perNodeProps[i] != null) {
                            for(final String[] kv: perNodeProps[i]) {
                                if(kv != null) {
                                    if(kv.length != 2)
                                        throw new IllegalArgumentException("Invalid KV Pair passed for per-node property");
                                    p2.set(kv[0], kv[1]);
                                }
                            }
                        }
                        final NodeManagerWithContext ret = makeNode(ctxs[i]);

                        // we can only do this level of polling when the min_nodes isn't set or is set to 1.
                        final String minNodesProp = System.getProperty("min_nodes");
                        if(minNodesProp != null && Integer.parseInt(minNodesProp) == 1)
                            assertTrue(qpoll(ret, o -> o.manager.isReady()));
                        return ret;
                    }
                })
                .collect(Collectors.toList());

            final List<NodeManagerWithContext> cpCtxs = reverseRange(0, reverseCpCtxs.size()).mapToObj(i -> reverseCpCtxs.get(i))
                .collect(Collectors.toList());

            for(final NodeManagerWithContext n: cpCtxs)
                assertTrue(poll(o -> n.manager.isReady()));

            test.test(new Nodes(cpCtxs, sessFact));
            currentlyTracking = null;
        } finally {
            LocalClusterSessionFactory.completeReset();
            BlockingQueueAddress.completeReset();
            ClassTracker.dumpResults();
        }

        test.postShutdown();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();

        for(final long endTime = System.currentTimeMillis() + TEN_SECONDS; Thread.activeCount() > 3;) {
            ignore(() -> Thread.sleep(10));
            if(Thread.activeCount() > 3 && (System.currentTimeMillis() > endTime)) {
                LOGGER.error("There appears to be an incomplete shutdown! There are {} threads running after stop.", Thread.activeCount());
                break;
            }
        }
    }

    @SuppressWarnings("resource")
    protected NodeManagerWithContext makeNode(final String[] ctxArr) {
        final List<String> fullCtx = new ArrayList<>(Arrays.asList(ctxArr));
        fullCtx.addAll(Arrays.asList(frameworkCtx));
        fullCtx.add(TRANSPORT_CTX_PREFIX + transportType + TRANSPORT_CTX_SUFFIX);
        if(!"none".equals(serializerType))
            fullCtx.add(SERIALIZER_CTX_PREFIX + serializerType + SERIALIZER_CTX_SUFFIX);
        LOGGER.debug("Starting node with {}", fullCtx);
        final ClassPathXmlApplicationContext ctx = currentlyTracking
            .track(new ClassPathXmlApplicationContext(fullCtx.toArray(new String[fullCtx.size()])));

        final ThreadingModel threading;
        currentlyTracking.track(threading = threadingModelSource.apply(currentAppName)).start("nodeid");

        return currentlyTracking.track(new NodeManagerWithContext(new NodeManager()
            .node(ctx.getBean(Node.class))
            .threadingModel(threading)
            .collaborator(uncheck(() -> currentSessionFactory.createSession()))
            .start(), ctx));
    }

}
