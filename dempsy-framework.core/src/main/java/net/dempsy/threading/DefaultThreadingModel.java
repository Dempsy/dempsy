package net.dempsy.threading;

import static net.dempsy.config.ConfigLogger.logConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.MessageDeliveryJob;
import net.dempsy.util.OccasionalRunnable;
import net.dempsy.util.SimpleExecutor;

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

    private SimpleExecutor executor = null;
    // private LinkedBlockingDeque<Runnable> priorityQueue = null;

    private final AtomicLong numLimited = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;
    private int threadPoolSize;

    private double m = Double.parseDouble(DEFAULT_CORES_FACTOR);
    private int additionalThreads = Integer.parseInt(DEFAULT_ADDITIONAL_THREADS);

    private final Supplier<String> nameSupplier;
    private boolean hardShutdown = Boolean.parseBoolean(DEFAULT_HARD_SHUTDOWN);

    private boolean blocking = Boolean.parseBoolean(DEFAULT_BLOCKING);
    private SubmitLimited submitter = null;

    private final AtomicLong threadNum = new AtomicLong(0L);
    private final static AtomicLong poolNum = new AtomicLong(0L);

    private boolean started = false;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    private final Runnable occLogger = OccasionalRunnable.staticOccasionalRunnable(LOG_QUEUE_LEN_MESSAGE_COUNT,
        () -> LOGGER.debug("Total messages pending on " + DefaultThreadingModel.class.getSimpleName() + ": {}",
            executor.getQueue().size()));

    public DefaultThreadingModel(final String threadNameBase) {
        this(threadNameBase, -1, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public DefaultThreadingModel(final String threadNameBase, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        final long curPoolNum = poolNum.getAndIncrement();
        this.nameSupplier = () -> threadNameBase + "-" + curPoolNum + "-" + threadNum.getAndIncrement();
        this.threadPoolSize = threadPoolSize;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
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
    public synchronized DefaultThreadingModel start(final String nodeid) {
        logConfig(LOGGER, "Threading Model {} for node: {}", DefaultThreadingModel.class.getName(), nodeid);
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

        // priorityQueue = new LinkedBlockingDeque<Runnable>() {
        // private static final long serialVersionUID = 1L;
        //
        // @Override
        // public boolean offer(final Runnable e) {
        // if(e instanceof PriorityRunnable)
        // return super.offerFirst(e);
        // return super.offer(e);
        // }
        //
        // };
        // executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS, priorityQueue, r -> new Thread(r, nameSupplier.get()));
        executor = new SimpleExecutor(threadPoolSize, r -> new Thread(r, nameSupplier.get()));

        if(blocking) {
            if(maxNumWaitingLimitedTasks > 0) // maxNumWaitingLimitedTasks <= 0 means unlimited
                submitter = new BlockingLimited(numLimited, executor, maxNumWaitingLimitedTasks, stopping);
            else {
                LOGGER.warn("You cannot configure \"" + CONFIG_KEY_BLOCKING + "\" and set \"" + CONFIG_KEY_MAX_PENDING
                    + "\" to unbounded at the same time. The queue will be unbounded.");
                submitter = new NonBlockingUnlimited(executor, stopping);
            }
        } else {
            if(maxNumWaitingLimitedTasks > 0) // maxNumWaitingLimitedTasks <= 0 means unlimited
                submitter = new NonBlockingLimited(numLimited, executor, maxNumWaitingLimitedTasks, stopping);
            else
                submitter = new NonBlockingUnlimited(executor, stopping);
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
        synchronized(this) {
            stopping.set(true);
        }
        if(hardShutdown) {
            if(executor != null) {
                submitter.skipping(executor.shutdownNow());
            }
        } else {
            if(executor != null)
                executor.shutdown();
        }
    }

    @Override
    public int getNumberLimitedPending() {
        return numLimited.intValue();
    }

    @Override
    public void submit(final MessageDeliveryJob r) {
        final var rejectable = new DefaultRejectable(r, stopping);
        if(!executor.submit(rejectable)) {
            LOGGER.warn("Regular job submission failed!");
            rejectable.reject();
        }
    }

    @Override
    public void submitPrioity(final MessageDeliveryJob r) {
        final var rejectable = new DefaultRejectable(r, stopping);
        if(!executor.submitFirst(rejectable)) {
            LOGGER.warn("Priority job submission failed!");
            rejectable.reject();
        }
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

    private static interface Rejectable extends Runnable {
        public void reject();
    }

    private static class DefaultRejectable implements Rejectable {
        final MessageDeliveryJob r;
        final AtomicBoolean stopping;

        public DefaultRejectable(final MessageDeliveryJob r, final AtomicBoolean stopping) {
            this.r = r;
            this.stopping = stopping;
        }

        @Override
        public void run() {
            doCall(r);
        }

        @Override
        public void reject() {
            r.rejected(stopping.get());
        }
    }

    private static class NonBlockingRejectable implements Rejectable {
        final MessageDeliveryJob r;
        final AtomicLong numLimited;
        final AtomicBoolean stopping;
        final long maxNumWaitingLimitedTasks;

        public NonBlockingRejectable(final MessageDeliveryJob r, final AtomicLong numLimited, final AtomicBoolean stopping,
            final long maxNumWaitingLimitedTasks) {
            this.r = r;
            this.numLimited = numLimited;
            this.stopping = stopping;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        }

        @Override
        public void run() {
            final long num = numLimited.decrementAndGet();
            if(num <= maxNumWaitingLimitedTasks)
                doCall(r);
            else
                r.rejected(stopping.get());
        }

        @Override
        public void reject() {
            numLimited.decrementAndGet();
            try {
                r.rejected(stopping.get());
            } catch(final RuntimeException rte) {
                LOGGER.warn("Rejecting a job resulted in an exception", rte);
            }
        }
    }

    private static class NonBlockingLimited implements SubmitLimited {
        private final AtomicLong numLimited;
        private final SimpleExecutor executor;
        private final long maxNumWaitingLimitedTasks;
        private final long twiceMaxNumWaitingLimitedTasks;
        private final AtomicBoolean stopping;;

        NonBlockingLimited(final AtomicLong numLimited, final SimpleExecutor executor, final long maxNumWaitingLimitedTasks, final AtomicBoolean stopping) {
            this.numLimited = numLimited;
            this.executor = executor;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            this.twiceMaxNumWaitingLimitedTasks = 2 * maxNumWaitingLimitedTasks;
            this.stopping = stopping;
        }

        @Override
        public void submitLimited(final MessageDeliveryJob r) {
            final long curCount = numLimited.incrementAndGet();

            if(curCount > twiceMaxNumWaitingLimitedTasks) {
                LOGGER.warn("We're at twice the number of acceptable pending messages {}(:{}). The system appears to be thread starved. Rejecting new message.",
                    curCount, executor.getQueue().size());
                numLimited.decrementAndGet();
                r.rejected(stopping.get());
            } else {
                final boolean submitOk;
                try {
                    submitOk = executor.submit(new NonBlockingRejectable(r, numLimited, stopping, maxNumWaitingLimitedTasks));
                } catch(final RuntimeException rte) {
                    LOGGER.warn("Limited job submission failed!", rte);
                    numLimited.decrementAndGet();
                    try {
                        r.rejected(stopping.get());
                    } catch(final RuntimeException rte2) {
                        LOGGER.warn("Failed rejecting job!", rte2);
                    }
                    throw rte;
                }

                if(!submitOk) {
                    numLimited.decrementAndGet();
                    LOGGER.warn("Limited job submission failed!");
                    try {
                        r.rejected(stopping.get());
                    } catch(final RuntimeException rte) {
                        LOGGER.warn("Failed rejecting job!", rte);
                        throw rte;
                    }
                }
            }
        }
    }

    private static class BlockingRejectable implements Rejectable {
        final MessageDeliveryJob r;
        final AtomicLong numLimited;
        final AtomicBoolean stopping;

        public BlockingRejectable(final MessageDeliveryJob r, final AtomicLong numLimited, final AtomicBoolean stopping) {
            this.r = r;
            this.numLimited = numLimited;
            this.stopping = stopping;
        }

        @Override
        public void run() {
            numLimited.decrementAndGet();
            doCall(r);
        }

        @Override
        public void reject() {
            numLimited.decrementAndGet();
            try {
                r.rejected(stopping.get());
            } catch(final RuntimeException rte) {
                LOGGER.warn("Rejecting a job resulted in an exception", rte);
            }
        }

    }

    private static class BlockingLimited implements SubmitLimited {
        private final AtomicLong numLimited;
        private final SimpleExecutor executor;
        private final long maxNumWaitingLimitedTasks;
        private final AtomicBoolean stopping;;

        BlockingLimited(final AtomicLong numLimited, final SimpleExecutor executor, final long maxNumWaitingLimitedTasks, final AtomicBoolean stopping) {
            this.numLimited = numLimited;
            this.executor = executor;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            this.stopping = stopping;
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

            if(!executor.submit(new BlockingRejectable(r, numLimited, stopping))) {
                numLimited.decrementAndGet();
                LOGGER.warn("Limited job submission failed!");
                r.rejected(stopping.get());
            }
        }
    }

    private static class NonBlockingUnlimited implements SubmitLimited {
        private final SimpleExecutor executor;
        private final AtomicBoolean stopping;

        NonBlockingUnlimited(final SimpleExecutor executor, final AtomicBoolean stopping) {
            this.executor = executor;
            this.stopping = stopping;
        }

        @Override
        public void submitLimited(final MessageDeliveryJob r) {
            final var rejectable = new DefaultRejectable(r, stopping);
            if(!executor.submit(rejectable)) {
                LOGGER.warn("Limited job submission failed!");
                rejectable.reject();
            }
        }
    }

    private static interface SubmitLimited {
        public void submitLimited(final MessageDeliveryJob r);

        public default void skipping(final List<Runnable> skipping) {
            skipping.stream()
                .map(r -> (Rejectable)r)
                .forEach(r -> r.reject());
        }
    }
}
