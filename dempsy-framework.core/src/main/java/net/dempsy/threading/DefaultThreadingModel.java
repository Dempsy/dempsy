package net.dempsy.threading;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.ContainerJob;

public class DefaultThreadingModel implements ThreadingModel {
    private static Logger LOGGER = LoggerFactory.getLogger(DefaultThreadingModel.class);

    private static final int minNumThreads = 1;

    public static final String CONFIG_KEY_MAX_PENDING = "max_pending";
    public static final String DEFAULT_MAX_PENDING = "100000";

    public static final String CONFIG_KEY_CORES_FACTOR = "cores_factor";
    public static final String DEFAULT_CORES_FACTOR = "1.0";

    public static final String CONFIG_KEY_ADDITIONAL_THREADS = "additional_threads";
    public static final String DEFAULT_ADDITIONAL_THREADS = "1";

    public static final String CONFIG_KEY_HARD_SHUTDOWN = "hard_shutdown";
    public static final String DEFAULT_HARD_SHUTDOWN = "true";

    public static final String CONFIG_KEY_BLOCKING = "blocking";
    public static final String DEFAULT_BLOCKING = "false";

    private ExecutorService executor = null;
    private final AtomicLong numLimited = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;
    private int threadPoolSize;

    private double m = Double.parseDouble(DEFAULT_CORES_FACTOR);
    private int additionalThreads = Integer.parseInt(DEFAULT_ADDITIONAL_THREADS);

    private final Supplier<String> nameSupplier;
    private boolean hardShutdown = Boolean.parseBoolean(DEFAULT_HARD_SHUTDOWN);

    private boolean blocking = Boolean.parseBoolean(DEFAULT_BLOCKING);
    private SubmitLimited submitter = null;

    private final static AtomicLong threadNum = new AtomicLong();
    private boolean started = false;

    private DefaultThreadingModel(final Supplier<String> nameSupplier, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.threadPoolSize = threadPoolSize;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
    }

    private DefaultThreadingModel(final Supplier<String> nameSupplier) {
        this(nameSupplier, -1, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    public DefaultThreadingModel(final String threadNameBase) {
        this(() -> threadNameBase + "-" + threadNum.getAndIncrement());
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public DefaultThreadingModel(final String threadNameBase, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this(() -> threadNameBase + "-" + threadNum.getAndIncrement(), threadPoolSize, maxNumWaitingLimitedTasks);
    }

    /**
     * <p>
     * Prior to calling start you can set the cores factor and additional cores. Ultimately the number of threads in the pool will be given by:
     * </p>
     *
     * <p>
     * num threads = m * num cores + b
     * </p>
     *
     * <p>
     * Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads
     * </p>
     */
    public DefaultThreadingModel setCoresFactor(final double m) {
        this.m = m;
        return this;
    }

    /**
     * <p>
     * Prior to calling start you can set the cores factor and additional cores. Ultimately the number of threads in the pool will be given by:
     * </p>
     *
     * <p>
     * num threads = m * num cores + b
     * </p>
     *
     * <p>
     * Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads
     * </p>
     */
    public DefaultThreadingModel setAdditionalThreads(final int additionalThreads) {
        this.additionalThreads = additionalThreads;
        return this;
    }

    /**
     * When closing this ThreadingModel, use a hard shutdown (shutdownNow) on the executors.
     */
    public DefaultThreadingModel setHardShutdown(final boolean hardShutdown) {
        this.hardShutdown = hardShutdown;
        return this;
    }

    /**
     * Blocking will cause {@link ThreadingModel#submitLimited(net.dempsy.threading.ThreadingModel.Rejectable, boolean)}
     * to block if the queue is filled.
     */
    public DefaultThreadingModel setBlocking(final boolean blocking) {
        this.blocking = blocking;
        return this;
    }

    public static String configKey(final String suffix) {
        return DefaultThreadingModel.class.getPackageName() + "." + suffix;
    }

    @Override
    public synchronized DefaultThreadingModel start() {
        if(threadPoolSize == -1) {
            // figure out the number of cores.
            final int cores = Runtime.getRuntime().availableProcessors();
            final int cpuBasedThreadCount = (int)Math.ceil(cores * m) + additionalThreads; // why? I don't know. If you don't like it
                                                                                           // then use the other constructor
            threadPoolSize = Math.max(cpuBasedThreadCount, minNumThreads);
        }
        executor = Executors.newFixedThreadPool(threadPoolSize, r -> new Thread(r, nameSupplier.get()));

        if(blocking) {
            if(maxNumWaitingLimitedTasks > 0) // maxNumWaitingLimitedTasks <= 0 means unlimited
                submitter = new BlockingLimited(numLimited, executor, maxNumWaitingLimitedTasks);
            else {
                LOGGER.warn("You cannot configure \"" + CONFIG_KEY_BLOCKING + "\" and set \"" + CONFIG_KEY_MAX_PENDING
                    + "\" to unbounded at the same time. The queue will be unbounded.");
                submitter = new NonBlockingUnlimited(executor);
            }
        } else {
            if(maxNumWaitingLimitedTasks > 0) // maxNumWaitingLimitedTasks <= 0 means unlimited
                submitter = new NonBlockingLimited(numLimited, executor, maxNumWaitingLimitedTasks);
            else
                submitter = new NonBlockingUnlimited(executor);
        }

        started = true;
        return this;
    }

    @Override
    public synchronized boolean isStarted() {
        return started;
    }

    private static String getConfigValue(final Map<String, String> conf, final String key, final String defaultValue) {
        final String entireKey = DefaultThreadingModel.class.getPackage().getName() + "." + key;
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    public DefaultThreadingModel configure(final Map<String, String> configuration) {
        setMaxNumberOfQueuedLimitedTasks(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_MAX_PENDING, DEFAULT_MAX_PENDING)));
        setHardShutdown(Boolean.parseBoolean(getConfigValue(configuration, CONFIG_KEY_HARD_SHUTDOWN, DEFAULT_HARD_SHUTDOWN)));
        setCoresFactor(Double.parseDouble(getConfigValue(configuration, CONFIG_KEY_CORES_FACTOR, DEFAULT_CORES_FACTOR)));
        setAdditionalThreads(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_ADDITIONAL_THREADS, DEFAULT_ADDITIONAL_THREADS)));
        setBlocking(Boolean.parseBoolean(getConfigValue(configuration, CONFIG_KEY_BLOCKING, DEFAULT_BLOCKING)));
        return this;
    }

    public int getMaxNumberOfQueuedLimitedTasks() {
        return (int)maxNumWaitingLimitedTasks;
    }

    public DefaultThreadingModel setMaxNumberOfQueuedLimitedTasks(final long maxNumWaitingLimitedTasks) {
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        return this;
    }

    @Override
    public void close() {
        if(hardShutdown) {
            if(executor != null)
                executor.shutdownNow();
        } else {
            if(executor != null)
                executor.shutdown();
        }
    }

    @Override
    public int getNumberLimitedPending() {
        return numLimited.intValue();
    }

    public boolean isRunning() {
        return (executor != null) &&
            !(executor.isShutdown() || executor.isTerminated());
    }

    @Override
    public void submit(final ContainerJob r) {
        executor.submit(r);
    }

    @Override
    public void submitLimited(final ContainerJob r) {
        submitter.submitLimited(r);
    }

    private static Object doCall(final ContainerJob r) throws Exception {
        if(!r.containersCalculated())
            r.calculateContainers();
        return r.call();
    }

    private static class NonBlockingLimited implements SubmitLimited {
        private final AtomicLong numLimited;
        private final ExecutorService executor;
        private final long maxNumWaitingLimitedTasks;

        NonBlockingLimited(final AtomicLong numLimited, final ExecutorService executor, final long maxNumWaitingLimitedTasks) {
            this.numLimited = numLimited;
            this.executor = executor;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        }

        @Override
        public Future<Object> submitLimited(final ContainerJob r) {
            // if it just arrived then we limit. Otherwise we just submit.
            numLimited.incrementAndGet();

            final Future<Object> ret = executor.submit(() -> {
                final long num = numLimited.decrementAndGet();
                if(num <= maxNumWaitingLimitedTasks) {
                    return doCall(r);
                }
                r.rejected();
                return null;
            });
            return ret;
        }
    }

    private static class BlockingLimited implements SubmitLimited {
        private final AtomicLong numLimited;
        private final ExecutorService executor;
        private final long maxNumWaitingLimitedTasks;

        BlockingLimited(final AtomicLong numLimited, final ExecutorService executor, final long maxNumWaitingLimitedTasks) {
            this.numLimited = numLimited;
            this.executor = executor;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        }

        @Override
        public Future<Object> submitLimited(final ContainerJob r) {
            // only goes in if I get a position less than the max.
            long spinner = 0;
            for(boolean done = false; !done;) {
                final long curValue = numLimited.get();
                if(curValue < maxNumWaitingLimitedTasks) {
                    if(numLimited.compareAndSet(curValue, curValue + 1L))
                        done = true;
                } else {
                    spinner++;
                    if(spinner < 1000)
                        Thread.yield();
                    else {
                        try {
                            Thread.sleep(1);
                        } catch(final InterruptedException ie) {}
                    }
                }
            }

            final Future<Object> ret = executor.submit(() -> {
                numLimited.decrementAndGet();
                return doCall(r);
            });
            return ret;
        }
    }

    private static class NonBlockingUnlimited implements SubmitLimited {
        private final ExecutorService executor;

        NonBlockingUnlimited(final ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public Future<Object> submitLimited(final ContainerJob r) {
            return executor.submit(() -> doCall(r));
        }
    }

    @FunctionalInterface
    private static interface SubmitLimited {
        public Future<Object> submitLimited(final ContainerJob r);
    }
}
