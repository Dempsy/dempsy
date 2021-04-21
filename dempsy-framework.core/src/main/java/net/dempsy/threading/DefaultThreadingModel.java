package net.dempsy.threading;

import static net.dempsy.config.ConfigLogger.logConfig;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.MessageDeliveryJob;
import net.dempsy.util.OccasionalRunnable;

public class DefaultThreadingModel implements ThreadingModel {
    private static Logger LOGGER = LoggerFactory.getLogger(DefaultThreadingModel.class);

    private static final int minNumThreads = 1;

    // when this anded (&) with the current message count is
    // zero, we'll log a message to the logger (as long as the log level is set appropriately).
    private static final long LOG_QUEUE_LEN_MESSAGE_COUNT = (1024 * 2);

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

    private final Runnable occLogger = OccasionalRunnable.staticOccasionalRunnable(LOG_QUEUE_LEN_MESSAGE_COUNT,
        () -> LOGGER.debug("Total messages pending on " + DefaultThreadingModel.class.getSimpleName() + ": {}",
            ((ThreadPoolExecutor)executor).getQueue().size()));

    private DefaultThreadingModel(final Supplier<String> nameSupplier, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.threadPoolSize = threadPoolSize;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
    }

    private DefaultThreadingModel(final Supplier<String> nameSupplier) {
        this(nameSupplier, -1, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    private static Supplier<String> bakedDefaultName(final String threadNameBase) {
        final long curTmNum = threadNum.getAndIncrement();
        return () -> threadNameBase + "-" + curTmNum;
    }

    public DefaultThreadingModel(final String threadNameBase) {
        this(bakedDefaultName(threadNameBase));
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public DefaultThreadingModel(final String threadNameBase, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this(bakedDefaultName(threadNameBase), threadPoolSize, maxNumWaitingLimitedTasks);
    }

    /**
     * <p>
     * Prior to calling start you can set the cores factor and additional cores.
     * Ultimately the number of threads in the pool will be given by:
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

    @Override
    public synchronized DefaultThreadingModel start() {
        logConfig(LOGGER, "Threading Model", DefaultThreadingModel.class.getName());
        logConfig(LOGGER, configKey(CONFIG_KEY_CORES_FACTOR), m, DEFAULT_CORES_FACTOR);
        logConfig(LOGGER, configKey(CONFIG_KEY_ADDITIONAL_THREADS), additionalThreads, DEFAULT_ADDITIONAL_THREADS);
        logConfig(LOGGER, configKey(CONFIG_KEY_MAX_PENDING), getMaxNumberOfQueuedLimitedTasks(), DEFAULT_MAX_PENDING);
        logConfig(LOGGER, configKey(CONFIG_KEY_HARD_SHUTDOWN), hardShutdown, DEFAULT_HARD_SHUTDOWN);
        logConfig(LOGGER, configKey(CONFIG_KEY_BLOCKING), blocking, DEFAULT_BLOCKING);

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
    public void submit(final MessageDeliveryJob r) {
        executor.submit(() -> r.executeAllContainers());
    }

    @Override
    public void submitLimited(final MessageDeliveryJob r) {
        if(LOGGER.isDebugEnabled())
            occLogger.run();
        submitter.submitLimited(r);
    }

    private static void doCall(final MessageDeliveryJob r) {
        if(!r.containersCalculated())
            r.calculateContainers();
        r.executeAllContainers();
    }

    private static class NonBlockingLimited implements SubmitLimited {
        private final AtomicLong numLimited;
        private final ExecutorService executor;
        private final long maxNumWaitingLimitedTasks;
        private final long twiceMaxNumWaitingLimitedTasks;

        NonBlockingLimited(final AtomicLong numLimited, final ExecutorService executor, final long maxNumWaitingLimitedTasks) {
            this.numLimited = numLimited;
            this.executor = executor;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            this.twiceMaxNumWaitingLimitedTasks = 2 * maxNumWaitingLimitedTasks;
        }

        @Override
        public void submitLimited(final MessageDeliveryJob r) {
            final long curCount = numLimited.incrementAndGet();

            if(curCount > twiceMaxNumWaitingLimitedTasks) {
                LOGGER.warn("We're at twice the number of acceptable pending messages. The system appears to be thread starved. Rejecting new message.");
                r.rejected();
            } else {
                executor.submit(() -> {
                    final long num = numLimited.decrementAndGet();
                    if(num <= maxNumWaitingLimitedTasks)
                        doCall(r);
                    else
                        r.rejected();
                });
            }
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
        public void submitLimited(final MessageDeliveryJob r) {
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

            executor.submit(() -> {
                numLimited.decrementAndGet();
                doCall(r);
            });
        }
    }

    private static class NonBlockingUnlimited implements SubmitLimited {
        private final ExecutorService executor;

        NonBlockingUnlimited(final ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void submitLimited(final MessageDeliveryJob r) {
            executor.submit(() -> doCall(r));
        }
    }

    @FunctionalInterface
    private static interface SubmitLimited {
        public void submitLimited(final MessageDeliveryJob r);
    }
}
