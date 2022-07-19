package net.dempsy.threading;

import static net.dempsy.config.ConfigLogger.logConfig;
import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.ignore;
import static net.dempsy.util.OccasionalRunnable.staticOccasionalRunnable;

import java.util.ArrayList;
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

// TODO: While this handles the maxPendingMessagesPerContainer correctly
// the maxNumWaitingLimitedTasks is effectively ignored.
public class OrderedPerContainerThreadingModel implements ThreadingModel {
    private static Logger LOGGER = LoggerFactory.getLogger(OrderedPerContainerThreadingModel.class);

    // when this anded (&) with the current message count is
    // zero, we'll log a message to the logger (as long as the log level is set appropriately).
    private static final long LOG_QUEUE_LEN_MESSAGE_COUNT = (1024 * 4);

    private static final int INTERIM_SPIN_COUNT1 = 100;
    private static final int INTERIM_SPIN_COUNT2 = 500;

    public static final String CONFIG_KEY_MAX_PENDING = "max_pending";
    public static final String DEFAULT_MAX_PENDING = "100000";

    public static final String CONFIG_KEY_DESERIALIZATION_THREADS = "deserialization_threads";
    public static final String DEFAULT_DESERIALIZATION_THREADS = "2";

    private static final AtomicLong seq = new AtomicLong(0);

    private ExecutorService calcContainersWork = null;

    private final BlockingDeque<MessageDeliveryJobHolder> inqueue = new LinkedBlockingDeque<>();
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private Thread shuttleThread = null;

    private final AtomicLong numLimited = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;
    private long maxNumWaitingLimitedTasksX2;
    public final boolean wereLimiting;

    private int deserializationThreadCount = Integer.parseInt(DEFAULT_DESERIALIZATION_THREADS);

    private final Supplier<String> nameSupplier;

    private final static AtomicLong threadNum = new AtomicLong();
    private boolean started = false;

    private OrderedPerContainerThreadingModel(final Supplier<String> nameSupplier, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        maxNumWaitingLimitedTasksX2 = maxNumWaitingLimitedTasks << 1;
        wereLimiting = (maxNumWaitingLimitedTasks > 0);
    }

    private OrderedPerContainerThreadingModel(final Supplier<String> nameSupplier) {
        this(nameSupplier, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    private static Supplier<String> bakedDefaultName(final String threadNameBase) {
        final long curTmNum = threadNum.getAndIncrement();
        return () -> threadNameBase + "-" + curTmNum;
    }

    public OrderedPerContainerThreadingModel(final String threadNameBase) {
        this(bakedDefaultName(threadNameBase));
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public OrderedPerContainerThreadingModel(final String threadNameBase, final int maxNumWaitingLimitedTasks) {
        this(bakedDefaultName(threadNameBase), maxNumWaitingLimitedTasks);
    }

    public OrderedPerContainerThreadingModel setDeserializationThreadCount(final int deserializationThreadCount) {
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
    private class ContainerWorker implements Runnable {
        public final LinkedBlockingDeque<ContainerJobHolder> queue;
        public final Container container;
        public final int maxPendingMessagesPerContainerX2;
        public final boolean shedMode;

        public ContainerWorker(final Container container) {
            this.container = container;
            final Container c = container;
            if(c.containerInternallyQueuesMessages())
                throw new IllegalArgumentException(
                    "Cannot use an " + OrderedPerContainerThreadingModel.class.getSimpleName() + " with a " + c.getClass().getSimpleName()
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

            this.queue = new LinkedBlockingDeque<>();

            chain(
                // this used to use the nameSupplier but the name is too long in `htop`
                // to understand what's going on so it's been switched to simple "c-"
                // (for "container") and the name of the cluster.
                new Thread(this, "c-" + container.getClusterId().clusterName),
                t -> t.start());
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
            // we can't really conditionally reject the oldest without disrupting the ordering guarantees
            // we we're going to throw away this one if we're at double our limits.
            if(curJobHolder.isLimited() && shedMode && (queue.size() > maxPendingMessagesPerContainerX2)) {
                curJobHolder.reject(container);
            } else if(!queue.offer(curJobHolder)) { // offer the job. If the job is queued then
                                                    // ownership of the lifecycle has been successfully
                                                    // passed to the container...

                // ... otherwise we need to make sure we mark the job as rejected at the container level.
                curJobHolder.reject(container);
                LOGGER.trace("Failed to be queued to container {}. The queue has {} messages in it",
                    container.getClusterId(), queue.size());
            }
        }

        @Override
        public void run() {
            long tryCount = 0;
            while(!isStopped.get()) {
                try {
                    final ContainerJobHolder job = queue.poll();
                    if(job != null) {
                        tryCount = 0;
                        // the "- 1" is because this job is being counted as on the
                        // queue right now until I either reject or process it. BUT
                        // I'd rather assume it's OFF the queue since, it will effectively
                        // is, it's just that it's bookeeping isn't done until reject/process
                        // is called.
                        //
                        // This also makes the bookeeping exactly consistent with the
                        // DefaultThreadingModel
                        if(job.isLimited() && (numLimited.get() - 1) > maxNumWaitingLimitedTasks)
                            job.reject(container);
                        else
                            job.process(container);
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
            drainTo.forEach(d -> d.reject(container));
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
                () -> LOGGER.debug("Total messages pending on {}: {}", OrderedPerContainerThreadingModel.class.getSimpleName(), inqueue.size()));

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
    public OrderedPerContainerThreadingModel start(final String nodeid) {
        logConfig(LOGGER, "Threading Model {} for node: {}", OrderedPerContainerThreadingModel.class.getSimpleName(), nodeid);
        logConfig(LOGGER, configKey(CONFIG_KEY_MAX_PENDING), getMaxNumberOfQueuedLimitedTasks(), DEFAULT_MAX_PENDING);
        logConfig(LOGGER, configKey(CONFIG_KEY_DESERIALIZATION_THREADS), deserializationThreadCount, DEFAULT_DESERIALIZATION_THREADS);

        shuttleThread = chain(newThread(new Shuttler(), nameSupplier.get() + "-Shuttle"), t -> t.start());
        // This is the executor that is running the deserialization (usually done in calculateContainers)
        // while the message is queued
        calcContainersWork = Executors.newFixedThreadPool(deserializationThreadCount,
            r -> new Thread(r, nameSupplier.get() + "-Deser-" + seq.getAndIncrement()));

        started = true;
        return this;
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
        this.maxNumWaitingLimitedTasksX2 = maxNumWaitingLimitedTasks << 1;
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
