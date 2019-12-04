package net.dempsy.threading;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.ignore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.Container;
import net.dempsy.container.ContainerJob;
import net.dempsy.container.ContainerJobMetadata;
import net.dempsy.util.SafeString;

public class OrderedPerContainerThreadingModel implements ThreadingModel {
    private static Logger LOGGER = LoggerFactory.getLogger(OrderedPerContainerThreadingModel.class);

    private static final int INTERIM_SPIN_COUNT1 = 100;
    private static final int INTERIM_SPIN_COUNT2 = 500;

    public static final String CONFIG_KEY_MAX_PENDING = "max_pending";
    public static final String DEFAULT_MAX_PENDING = "100000";

    public static final String CONFIG_KEY_HARD_SHUTDOWN = "hard_shutdown";
    public static final String DEFAULT_HARD_SHUTDOWN = "true";

    public static final String CONFIG_KEY_DESERIALIZATION_THREADS = "deserialization_threads";
    public static final String DEFAULT_DESERIALIZATION_THREADS = "2";

    private static final AtomicLong seq = new AtomicLong(0);

    private ExecutorService calcContainersWork = null;

    private final BlockingQueue<ContainerJobHolder> inqueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ContainerJobHolder> deserQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private Thread shuttleThread = null;

    private final AtomicLong numLimitedX = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;

    private int deserializationThreadCount = Integer.parseInt(DEFAULT_DESERIALIZATION_THREADS);

    private final Supplier<String> nameSupplier;

    private final static AtomicLong threadNum = new AtomicLong();
    private boolean started = false;

    public OrderedPerContainerThreadingModel(final Supplier<String> nameSupplier, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
    }

    public OrderedPerContainerThreadingModel(final Supplier<String> nameSupplier) {
        this(nameSupplier, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    public OrderedPerContainerThreadingModel(final String threadNameBase) {
        this(() -> threadNameBase + "-" + threadNum.getAndIncrement());
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public OrderedPerContainerThreadingModel(final String threadNameBase, final int maxNumWaitingLimitedTasks) {
        this(() -> threadNameBase + "-" + threadNum.getAndIncrement(), maxNumWaitingLimitedTasks);
    }

    public OrderedPerContainerThreadingModel setDeserializationThreadCount(final int deserializationThreadCount) {
        this.deserializationThreadCount = deserializationThreadCount;
        return this;
    }

    public static String configKey(final String suffix) {
        return OrderedPerContainerThreadingModel.class.getPackageName() + "." + suffix;
    }

    @Override
    public synchronized boolean isStarted() {
        return started;
    }

    private static class ContainerJobHolder {
        private final ContainerJob jobX;
        private final boolean limited;
        private final AtomicLong numLimited;
        private final long maxNumWaitingLimitedTasks;

        public ContainerJobHolder(final ContainerJob job, final boolean limited, final AtomicLong numLimited, final long maxNumWaitingLimitedTasks) {
            this.jobX = job;
            this.limited = limited;
            this.numLimited = numLimited;
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            if(limited)
                numLimited.incrementAndGet();
        }

        public final void process() throws Exception {
            // System.out.println("Processing message to " + Arrays.stream(jobX.containerData()).map(d -> d.c.getClusterId()).collect(Collectors.toList()));
            if(limited) {
                final long num = numLimited.decrementAndGet();
                if(num <= maxNumWaitingLimitedTasks)
                    jobX.call();
                else
                    jobX.rejected();
            } else
                jobX.call();
        }

        public final void reject() {
            if(limited)
                numLimited.decrementAndGet();
            jobX.rejected();
        }

        public final boolean areContainersCalculated() {
            return jobX.containersCalculated();
        }

        public final void calculateContainers() {
            jobX.calculateContainers();
        }
    }

    private class ContainerWorker implements Runnable {
        public final BlockingQueue<ContainerJobHolder> queue;
        public final Thread containerThread;
        public final Container container;

        public ContainerWorker(final Container container) {
            this.queue = new LinkedBlockingQueue<>();
            this.container = container;
            containerThread = chain(new Thread(this, nameSupplier.get() + "-ContainerWorker-" + seq.incrementAndGet()),
                t -> t.start());
        }

        @Override
        public void run() {
            int tryCount = 0;
            while(!isStopped.get()) {
                try {
                    final ContainerJobHolder jobh = queue.poll();
                    if(jobh != null) {
                        tryCount = 0;
                        jobh.process();
                    } else {
                        // spin down
                        tryCount++;
                        if(tryCount > INTERIM_SPIN_COUNT2)
                            ignore(() -> Thread.sleep(1));
                        else if(tryCount > INTERIM_SPIN_COUNT1)
                            Thread.yield();
                    }
                } catch(final Throwable th) {
                    LOGGER.error("Completely unexpected exception:", th);
                }
            }

            // if we got here then we're shutting down ... we need to account for all of the queued jobs.
            final List<ContainerJobHolder> drainTo = new ArrayList<>(queue.size());
            queue.drainTo(drainTo);
            drainTo.forEach(d -> d.reject());
        }
    }

    private class Shuttler implements Runnable {

        private final Map<Container, ContainerWorker> containerWorkers = new HashMap<>();

        private void handleCalculatedContainerMessage(final ContainerJobHolder message) {
            // the message should have containers ...
            final ContainerJobMetadata[] deliveries = message.jobX.containerData();
            // ... but just to double check.
            if(deliveries != null && deliveries.length > 0) {
                Arrays.stream(deliveries)
                    // container
                    .map(d -> d.c)
                    // this is a single thread so this should be safe.
                    // mapped to queue
                    .map(c -> containerWorkers.computeIfAbsent(c, x -> new ContainerWorker(c)))
                    .map(cw -> cw.queue)
                    // mapped to whether or not the message could be inserted
                    .map(lbq -> lbq.offer(message))
                    // selecting out the failures
                    .filter(v -> v.booleanValue() == false)
                    // ... in order to log.
                    // The reason we aren't "rejecting" the message is because we only want to do that
                    // when there's a failure to deliver to ANY container.
                    .forEach(v -> LOGGER.warn("Message {} failed to be inserted into unbounded queue.", SafeString.objectDescription(message)));
            } else {
                // the message should have had containers but didn't
                LOGGER.info("Message didn't deserialize correctly.");
                message.reject();
            }
        }

        @Override
        public void run() {
            int tryCount = 0;
            while(!isStopped.get()) {
                boolean someWorkDone = false;
                // ========================================================
                // Phase I of this event loop:
                // check the inqueue.
                // ========================================================
                try {
                    final ContainerJobHolder message = inqueue.poll();
                    if(message != null) {
                        // there's work to be done.
                        someWorkDone = true;
                        tryCount = 0;
                        if(message.areContainersCalculated()) {
                            handleCalculatedContainerMessage(message);
                        } else {
                            if(!deserQueue.offer(message))
                                message.reject();
                            else
                                calcContainersWork.submit(() -> {
                                    message.calculateContainers();
                                });
                        }
                    }
                } catch(final RuntimeException rte) {
                    LOGGER.error("Error while dequeing.", rte);
                }

                // ========================================================
                // Phase II of this event loop:
                // check if the deserialization's been done
                // ========================================================
                try {
                    // peek into the next queue entry to see if the it's deserialized yet.
                    final ContainerJobHolder peeked = deserQueue.peek();
                    if(peeked != null && peeked.areContainersCalculated()) {
                        // we're the only thread reading this queue ...
                        final ContainerJobHolder message = deserQueue.poll();
                        // ... so 'message' should be the peeked value ...
                        // ... therefore it can't be null and must have containersCalculated == true
                        someWorkDone = true;
                        tryCount = 0;
                        handleCalculatedContainerMessage(message);
                    }
                } catch(final RuntimeException rte) {
                    LOGGER.error("Error while dequeing.", rte);
                }

                if(!someWorkDone) {
                    tryCount++;
                    if(tryCount > INTERIM_SPIN_COUNT2)
                        ignore(() -> Thread.sleep(1));
                    else if(tryCount > INTERIM_SPIN_COUNT1) {
                        Thread.yield();
                    }
                }
            }
        }
    }

    @Override
    public OrderedPerContainerThreadingModel start() {
        shuttleThread = chain(newThread(new Shuttler(), nameSupplier.get() + "-Shuttle"), t -> t.start());
        calcContainersWork = Executors.newFixedThreadPool(deserializationThreadCount,
            r -> new Thread(r, nameSupplier.get() + "-Deser-" + seq.getAndIncrement()));

        started = true;
        return this;
    }

    private static String getConfigValue(final Map<String, String> conf, final String key, final String defaultValue) {
        final String entireKey = OrderedPerContainerThreadingModel.class.getPackage().getName() + "." + key;
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    public OrderedPerContainerThreadingModel configure(final Map<String, String> configuration) {
        setMaxNumberOfQueuedLimitedTasks(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_MAX_PENDING, DEFAULT_MAX_PENDING)));
        setDeserializationThreadCount(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_DESERIALIZATION_THREADS, DEFAULT_DESERIALIZATION_THREADS)));
        return this;
    }

    public int getMaxNumberOfQueuedLimitedTasks() {
        return (int)maxNumWaitingLimitedTasks;
    }

    public OrderedPerContainerThreadingModel setMaxNumberOfQueuedLimitedTasks(final long maxNumWaitingLimitedTasks) {
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        return this;
    }

    @Override
    public void close() {
        isStopped.set(true);
        ignore(() -> shuttleThread.join(10000));
        if(shuttleThread.isAlive())
            LOGGER.warn("Couldn't stop the dequeing thread.");
    }

    @Override
    public int getNumberLimitedPending() {
        return numLimitedX.intValue();
    }

    @Override
    public void submit(final ContainerJob job) {
        final ContainerJobHolder jobh = new ContainerJobHolder(job, false, numLimitedX, maxNumWaitingLimitedTasks);
        if(!inqueue.offer(jobh)) {
            jobh.reject();
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .map(c -> c.c)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }

    @Override
    public void submitLimited(final ContainerJob job) {
        final ContainerJobHolder jobh = new ContainerJobHolder(job, true, numLimitedX, maxNumWaitingLimitedTasks);
        if(!inqueue.offer(jobh)) {
            jobh.reject();// undo, since we failed. Though this should be impossible
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .map(c -> c.c)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }
}
