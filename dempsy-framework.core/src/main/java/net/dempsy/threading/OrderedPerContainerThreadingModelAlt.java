package net.dempsy.threading;

import static net.dempsy.config.ConfigLogger.logConfig;
import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.ignore;
import static net.dempsy.util.OccasionalRunnable.staticOccasionalRunnable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.Container;
import net.dempsy.container.ContainerJob;
import net.dempsy.container.MessageDeliveryJob;
import net.dempsy.util.GroupExecutor;

/**
 * This threading model provides some ordering guarantees where the default
 * thread model doesn't. The default threading model can have multiple threads
 * race to provide message processing from a central queue and therefore
 * can end up submitting messages to the container in a different order then
 * they were submitted to the central queue. This threading model will guarantee
 * that messages submitted to a specific container in a particular order will
 * be delivered in that order.
 */
// TODO: While this handles the maxPendingMessagesPerContainer correctly
// the maxNumWaitingLimitedTasks is effectively ignored.
public class OrderedPerContainerThreadingModelAlt implements ThreadingModel {
    private static Logger LOGGER = LoggerFactory.getLogger(OrderedPerContainerThreadingModelAlt.class);

    private static final int minNumThreads = 1;

    // when this anded (&) with the current message count is
    // zero, we'll log a message to the logger (as long as the log level is set appropriately).
    private static final long LOG_QUEUE_LEN_MESSAGE_COUNT = (1024 * 4);

    private static final int INTERIM_SPIN_COUNT1 = 100;
    private static final int INTERIM_SPIN_COUNT2 = 500;

    public static final String CONFIG_KEY_MAX_PENDING = "max_pending";
    public static final String DEFAULT_MAX_PENDING = "100000";

    public static final String CONFIG_KEY_DESERIALIZATION_THREADS = "deserialization_threads";
    public static final String DEFAULT_DESERIALIZATION_THREADS = "2";

    public static final String CONFIG_KEY_CORES_FACTOR = "cores_factor";
    public static final String DEFAULT_CORES_FACTOR = "1.0";

    public static final String CONFIG_KEY_ADDITIONAL_THREADS = "additional_threads";
    public static final String DEFAULT_ADDITIONAL_THREADS = "1";

    private static final AtomicLong seq = new AtomicLong(0);

    private ExecutorService calcContainersWork = null;

    private final BlockingDeque<MessageDeliveryJobHolder> inqueue = new LinkedBlockingDeque<>();
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private Thread shuttleThread = null;

    private final AtomicLong numLimited = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;
    private long maxNumWaitingLimitedTasksX2;

    private int deserializationThreadCount = Integer.parseInt(DEFAULT_DESERIALIZATION_THREADS);

    private final Supplier<String> nameSupplier;

    private final static AtomicLong threadNum = new AtomicLong();
    private boolean started = false;
    private final static AtomicLong poolNum = new AtomicLong(0L);
    private GroupExecutor groupExecutor = null;

    private double m = Double.parseDouble(DEFAULT_CORES_FACTOR);
    private int additionalThreads = Integer.parseInt(DEFAULT_ADDITIONAL_THREADS);
    private final int threadPoolSize;

    public OrderedPerContainerThreadingModelAlt(final String threadNameBase) {
        this(threadNameBase, -1, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    /**
     * Create a OrderedPerContainerThreadingModelAlt with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public OrderedPerContainerThreadingModelAlt(final String threadNameBase, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        final long curPoolNum = poolNum.getAndIncrement();
        this.nameSupplier = () -> threadNameBase + "-" + curPoolNum + "-" + threadNum.getAndIncrement();;
        if(maxNumWaitingLimitedTasks < 0) { // unbounded
            this.maxNumWaitingLimitedTasks = Long.MAX_VALUE;
            this.maxNumWaitingLimitedTasksX2 = Long.MAX_VALUE;
        } else {
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            this.maxNumWaitingLimitedTasksX2 = maxNumWaitingLimitedTasks << 1;
        }
        this.threadPoolSize = threadPoolSize;
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
    public OrderedPerContainerThreadingModelAlt setCoresFactor(final double m) {
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
    public OrderedPerContainerThreadingModelAlt setAdditionalThreads(final int additionalThreads) {
        this.additionalThreads = additionalThreads;
        return this;
    }

    public OrderedPerContainerThreadingModelAlt setDeserializationThreadCount(final int deserializationThreadCount) {
        this.deserializationThreadCount = deserializationThreadCount;
        return this;
    }

    @Override
    public synchronized boolean isStarted() {
        return started;
    }

    /**
     * The main goal of this class is to represent the individuated instance
     * that may is on the queue to a single container. This is in contrast
     * to the MessageDeliveryJobHolder which is the ONE instance that represents
     * all of the individuated jobs on the individual container queues or potentially
     * waiting to be put on those queues.
     */
    private static class ContainerJobHolder {
        private final ContainerJob job;
        private final MessageDeliveryJobHolder wholeJob;

        public ContainerJobHolder(final ContainerJob job, final MessageDeliveryJobHolder jobHolder) {
            this.job = job;
            this.wholeJob = jobHolder;
            wholeJob.preEnqueuedTrackContainerJob();
        }

        public void process(final Container jobData) {
            wholeJob.preWorkTrackContainerJob();
            try {
                job.execute(jobData);
            } finally {
                wholeJob.postWorkTrackContainerJob();
            }
        }

        public void reject(final Container jobData) {
            wholeJob.preWorkTrackContainerJob();
            try {
                job.reject(jobData);
            } finally {
                wholeJob.postWorkTrackContainerJob();
            }
        }

        public boolean isLimited() {
            return wholeJob.limited;
        }
    }

    /**
     * The main goal of this class is to represent the ONE instance
     * that may be put on multiple container specific queues. The
     * ContainerJobHolder is the instance that represents the
     * "individuated" job on the queue to a single container.
     */
    private static class MessageDeliveryJobHolder {
        private final MessageDeliveryJob job;
        private final boolean limited;
        private final AtomicLong numLimited;

        // This is used to track the individuated jobs and decrement
        // the numLimited when the last one comes off the queue to be
        // processed or rejected by the container.
        private final AtomicLong numOfMeEnqueued = new AtomicLong(0L);

        // *******************************************************************
        // this tracks the number of times THIS JOB is either in the queue for
        // a specific container OR is being worked by a container. The main goal
        // of this is to track "individulated" jobs and call individuatedJobsComplete
        // once all of the containers this message was destined for have been handled
        // by the containers.
        private final AtomicLong unfinishedContainerJobs = new AtomicLong(0);
        // *******************************************************************

        private final AtomicBoolean stopping;

        public MessageDeliveryJobHolder(final MessageDeliveryJob job, final boolean limited, final AtomicLong numLimited, final AtomicBoolean stopping) {
            this.job = job;
            this.limited = limited;
            this.numLimited = numLimited;
            this.stopping = stopping;
        }

        public final void reject() {
            job.rejected(stopping.get());
        }

        public final boolean areContainersCalculated() {
            return job.containersCalculated();
        }

        public final void calculateContainers() {
            job.calculateContainers();
        }

        /**
         * This is called as the job is being placed on the queue
         * to the container.
         */
        public final void preEnqueuedTrackContainerJob() {
            numOfMeEnqueued.incrementAndGet();
            unfinishedContainerJobs.incrementAndGet();
        }

        public final void preWorkTrackContainerJob() {
            if(numOfMeEnqueued.decrementAndGet() == 0) {
                if(limited)
                    numLimited.decrementAndGet();
            }
        }

        public final void postWorkTrackContainerJob() {
            if(unfinishedContainerJobs.decrementAndGet() == 0) {
                // everything with this message is done. decrement any resources.
                job.individuatedJobsComplete();
            }
        }
    }

    /**
     * This object is the runnable for the thread that is the container's thread.
     * It has it's own queue which is specific to the container it's managing.
     */
    private class ContainerWorker {
        private final GroupExecutor.Queue queue;
        private final Container containerX;
        private final int maxPendingMessagesPerContainerX2;
        private final boolean shedMode;

        public ContainerWorker(final Container container) {
            this.containerX = container;
            final Container c = container;
            if(c.containerInternallyQueuesMessages())
                throw new IllegalArgumentException(
                    "Cannot use an " + OrderedPerContainerThreadingModelAlt.class.getSimpleName() + " with a " + c.getClass().getSimpleName()
                        + " container, as is being done for the cluster \"" + c.getClusterId().clusterName
                        + "\" because it internally queues messages, defeating the only reason to use this threading model.");
            maxPendingMessagesPerContainerX2 = c.getMaxPendingMessagesPerContainer() * 2;
            if(maxPendingMessagesPerContainerX2 <= 0) {
                LOGGER.warn(
                    "The container for \"{}\" has no limit set on the number of maximum queued messages. If the processing thread hangs up the messages will queue indefinitely potentially causing the process to run out of memory.",
                    c.getClusterId().clusterName);
                shedMode = false;
            } else
                shedMode = true;

            // this.queue = new LinkedBlockingDeque<>();
            this.queue = groupExecutor.newExecutor();
        }

        /**
         * Because the OrderedPerContainerThreadingModel uses a dedicated thread per container,
         * the message processor can block that thread through IO or some long running calculation.
         * In that case the Shuttle thread will perpetually enqueue messages resulting in a
         * potential to run out of memory. <strike>This method will dequeue the oldest messages in the
         * queue IFF the size of the queue grows to TWICE the maxPendingMessagesPerContainer
         * set on the container.</strike>
         */
        public void handleEnqueuing(final ContainerJobHolder curJobHolder) {
            // We can't really conditionally reject the oldest without disrupting the ordering guarantees.
            // We're going to throw away this one if we're at double our limits.
            if(curJobHolder.isLimited() && shedMode &&
                (queue.size() > maxPendingMessagesPerContainerX2)) {
                curJobHolder.reject(containerX);
            } else if(!queue.submit(() -> {
                if(curJobHolder.isLimited() && (numLimited.get() - 1) > maxNumWaitingLimitedTasks)
                    curJobHolder.reject(containerX);
                else
                    curJobHolder.process(containerX);
            })) { // offer the job. If the job is queued then
                  // ownership of the lifecycle has been successfully
                  // passed to the container...

                // ... otherwise we need to make sure we mark the job as rejected at the container level.
                curJobHolder.reject(containerX);
                LOGGER.trace("Failed to be queued to container {}. The queue has {} messages in it",
                    containerX.getClusterId(), queue.size());
            }
        }
    }

    private class Shuttler implements Runnable {

        private final Map<Container, ContainerWorker> containerWorkers = new HashMap<>();
        private final BlockingQueue<MessageDeliveryJobHolder> deserQueue = new LinkedBlockingQueue<>();

        private void handleCalculatedContainerMessage(final MessageDeliveryJobHolder message) {
            // the message should have containers ...
            final Container[] deliveries = message.job.containerData();

            // ... but just to double check.
            if(deliveries != null && deliveries.length > 0) {
                // these MUST be either processed or rejected. The "individuate" call
                // increments reference counts for each returned containerJobs so these
                // need to have their lifecycle fully managed.
                final List<ContainerJob> containerJobs = message.job.individuate();

                // First we need to construct the holders. If we enqueue them in the same list there's a race condition
                // where the job can finish before the the next one is enqueued and if that happens then the post
                // processing is called while there's still messages to be delivered.
                //
                // Instantiating the ContainerJobHolders creates the bookeeping for knowing when everything is complete.
                final ContainerJobHolder[] cjholders = containerJobs.stream().map(cj -> new ContainerJobHolder(cj, message))
                    .toArray(ContainerJobHolder[]::new);

                int i = 0;
                for(final ContainerJobHolder curJobHolder: cjholders) {
                    final Container container = deliveries[i];
                    // this is a single thread so this should be safe. The Function<> is NOT side effect free. It starts a thread.
                    final ContainerWorker curWorker = containerWorkers.computeIfAbsent(container, x -> new ContainerWorker(container));

                    curWorker.handleEnqueuing(curJobHolder);

                    i++;
                }
            } else {
                // the message should have had containers but didn't
                LOGGER.info("Message didn't deserialize correctly.");
                message.reject();
            }
        }

        @Override
        public void run() {
            int tryCount = 0;

            // This is used to log the message
            final Runnable occLogger = staticOccasionalRunnable(LOG_QUEUE_LEN_MESSAGE_COUNT,
                () -> LOGGER.debug("Total messages pending on {}: {}", OrderedPerContainerThreadingModelAlt.class.getSimpleName(), inqueue.size()));

            while(!isStopped.get()) {
                // this flag just helps the spin-lock spin
                // down with the loop does nothing.
                boolean someWorkDone = false;

                // ========================================================
                // Phase I of this event loop:
                // check the inqueue.
                // ========================================================
                try {
                    final MessageDeliveryJobHolder message = inqueue.poll();

                    // before we do anything, if we're twice the acceptable maxNumWaitingLimitedTasks and this is
                    // a limited task, we vent it.
                    if(message != null) {
                        if(message.limited && numLimited.get() > maxNumWaitingLimitedTasksX2) {
                            message.preEnqueuedTrackContainerJob();
                            message.preWorkTrackContainerJob();
                            message.reject();
                            message.postWorkTrackContainerJob();
                        } else {
                            if(LOGGER.isDebugEnabled())
                                occLogger.run();

                            // there's work to be done. Reset the spin lock
                            // and make sure we skip the spin the next time
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
                    }
                } catch(final RuntimeException rte) {
                    LOGGER.error("Error while dequeing.", rte);
                }

                // ========================================================
                // Phase II of this event loop:
                // check if the deserialization's been done
                // ========================================================
                try {
                    // we want to leave the message on the queue if it's not done
                    // being deserialized yet.

                    // peek into the next queue entry to see if the it's deserialized yet.
                    final MessageDeliveryJobHolder peeked = deserQueue.peek();
                    if(peeked != null && peeked.areContainersCalculated()) {
                        // we're the only thread reading this queue ...
                        final MessageDeliveryJobHolder message = deserQueue.poll();
                        // ... so 'message' should be the peeked value ...
                        // ... therefore it can't be null and must have containersCalculated == true

                        // there's work to be done. Reset the spin lock
                        // and make sure we skip the spin the next time
                        someWorkDone = true;
                        tryCount = 0;

                        handleCalculatedContainerMessage(message);
                    }
                } catch(final RuntimeException rte) {
                    LOGGER.error("Error while dequeing.", rte);
                }

                // If we didn't do anything then spin the lock once.
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
    public OrderedPerContainerThreadingModelAlt start(final String nodeid) {
        logConfig(LOGGER, "Threading Model {} for node: {}", OrderedPerContainerThreadingModelAlt.class.getSimpleName(), nodeid);
        logConfig(LOGGER, configKey(CONFIG_KEY_MAX_PENDING), getMaxNumberOfQueuedLimitedTasks(), DEFAULT_MAX_PENDING);
        logConfig(LOGGER, configKey(CONFIG_KEY_DESERIALIZATION_THREADS), deserializationThreadCount, DEFAULT_DESERIALIZATION_THREADS);

        int ctps = threadPoolSize;
        if(ctps == -1) {
            // figure out the number of cores.
            final int cores = Runtime.getRuntime().availableProcessors();
            final int cpuBasedThreadCount = (int)Math.ceil(cores * m) + additionalThreads; // why? I don't know. If you don't like it
                                                                                           // then use the other constructor
            ctps = Math.max(cpuBasedThreadCount, minNumThreads);
        }

        groupExecutor = new GroupExecutor(ctps, r -> new Thread(r, nameSupplier.get()));

        shuttleThread = chain(newThread(new Shuttler(), nameSupplier.get() + "-Shuttle"), t -> t.start());
        // This is the executor that is running the deserialization (usually done in calculateContainers)
        // while the message is queued
        calcContainersWork = Executors.newFixedThreadPool(deserializationThreadCount,
            r -> new Thread(r, nameSupplier.get() + "-Deser-" + seq.getAndIncrement()));

        started = true;
        return this;
    }

    public OrderedPerContainerThreadingModelAlt configure(final Map<String, String> configuration) {
        setMaxNumberOfQueuedLimitedTasks(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_MAX_PENDING, DEFAULT_MAX_PENDING)));
        setDeserializationThreadCount(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_DESERIALIZATION_THREADS, DEFAULT_DESERIALIZATION_THREADS)));
        return this;
    }

    public int getMaxNumberOfQueuedLimitedTasks() {
        return (int)maxNumWaitingLimitedTasks;
    }

    public OrderedPerContainerThreadingModelAlt setMaxNumberOfQueuedLimitedTasks(final long maxNumWaitingLimitedTasks) {
        if(maxNumWaitingLimitedTasks < 0) { // unbounded
            this.maxNumWaitingLimitedTasks = Long.MAX_VALUE;
            this.maxNumWaitingLimitedTasksX2 = Long.MAX_VALUE;
        } else {
            this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
            this.maxNumWaitingLimitedTasksX2 = maxNumWaitingLimitedTasks << 1;
        }
        return this;
    }

    @Override
    public void close() {
        isStopped.set(true);
        ignore(() -> shuttleThread.join(10000));
        if(shuttleThread.isAlive())
            LOGGER.warn("Couldn't stop the dequeing thread.");
        if(calcContainersWork != null)
            calcContainersWork.shutdown();

        for(final long endTime = System.currentTimeMillis() + 5000; System.currentTimeMillis() < endTime;) {
            if(calcContainersWork.isTerminated())
                break;
            ignore(() -> Thread.sleep(100));
        }
        if(!calcContainersWork.isTerminated())
            calcContainersWork.shutdownNow();

        groupExecutor.shutdownNow();
    }

    @Override
    public int getNumberLimitedPending() {
        return numLimited.intValue();
    }

    @Override
    public void submit(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, false, numLimited, isStopped);
        if(!inqueue.offer(jobh)) {
            jobh.reject();
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }

    @Override
    public void submitPrioity(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, false, numLimited, isStopped);
        if(!inqueue.offerFirst(jobh)) {
            jobh.reject();
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }

    @Override
    public void submitLimited(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, true, numLimited, isStopped);
        if(!inqueue.offer(jobh)) {
            jobh.reject();// undo, since we failed. Though this should be impossible
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        } else
            numLimited.incrementAndGet();
    }
}
