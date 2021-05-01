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
import net.dempsy.container.ContainerJobMetadata;
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

    private final AtomicLong numLimitedX = new AtomicLong(0);
    private long maxNumWaitingLimitedTasks;

    private int deserializationThreadCount = Integer.parseInt(DEFAULT_DESERIALIZATION_THREADS);

    private final Supplier<String> nameSupplier;

    private final static AtomicLong threadNum = new AtomicLong();
    private boolean started = false;

    private OrderedPerContainerThreadingModel(final Supplier<String> nameSupplier, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
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

    private static class ContainerJobHolder {
        private final ContainerJob job;
        private final MessageDeliveryJobHolder wholeJob;

        public ContainerJobHolder(final ContainerJob job, final MessageDeliveryJobHolder jobHolder) {
            this.job = job;
            this.wholeJob = jobHolder;
            wholeJob.preEnqueuedTrackContainerJob();
        }

        public void process(final ContainerJobMetadata jobData) {
            wholeJob.preWorkTrackContainerJob();
            try {
                job.execute(jobData);
            } finally {
                wholeJob.postWorkTrackContainerJob();
            }
        }

        public void reject(final ContainerJobMetadata jobData) {
            wholeJob.preWorkTrackContainerJob();
            try {
                job.reject(jobData);
            } finally {
                wholeJob.postWorkTrackContainerJob();
            }
        }
    }

    private static class MessageDeliveryJobHolder {
        private final MessageDeliveryJob job;
        private final boolean limited;
        private final AtomicLong numLimited;
        private final AtomicLong queuedContainerJobsX = new AtomicLong(0);
        private final AtomicLong unfinishedContainerJobsX = new AtomicLong(0);

        public MessageDeliveryJobHolder(final MessageDeliveryJob job, final boolean limited, final AtomicLong numLimited,
            final long maxNumWaitingLimitedTasks) {
            this.job = job;
            this.limited = limited;
            this.numLimited = numLimited;
            if(limited)
                numLimited.incrementAndGet();
        }

        public final void reject() {
            if(limited)
                numLimited.decrementAndGet();
            job.rejected();
        }

        public final boolean areContainersCalculated() {
            return job.containersCalculated();
        }

        public final void calculateContainers() {
            job.calculateContainers();
        }

        public final void preEnqueuedTrackContainerJob() {
            queuedContainerJobsX.incrementAndGet();
            unfinishedContainerJobsX.incrementAndGet();
        }

        public final void preWorkTrackContainerJob() {
            if(queuedContainerJobsX.decrementAndGet() == 0) {
                // we're about to execute (or reject) the final container job from this individuated message
                // so lets now discount it from the total queue length.
                if(limited)
                    numLimited.decrementAndGet();
            }
        }

        public final void postWorkTrackContainerJob() {
            if(unfinishedContainerJobsX.decrementAndGet() == 0) {
                // everything with this message is done. decrement any resources.
                job.individuatedJobsComplete();
            }
        }
    }

    private class ContainerWorker implements Runnable {
        public final LinkedBlockingQueue<ContainerJobHolder> queue;
        public final ContainerJobMetadata container;
        public final int maxPendingMessagesPerContainerX2;
        public final boolean shedMode;

        public ContainerWorker(final ContainerJobMetadata container) {
            this.container = container;
            final Container c = container.container;
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

            this.queue = new LinkedBlockingQueue<>();

            chain(
                // this used to use the nameSupplier but the name is too long in `htop`
                // to understand what's going on so it's been switched to simple "c-"
                // (for "container") and the name of the cluster.
                new Thread(this, "c-" + container.container.getClusterId().clusterName),
                t -> t.start());
        }

        /**
         * Because the OrderedPerContainerThreadingModel uses a dedicated thread per container,
         * the message processor can block that thread through IO or some long running calculation.
         * In that case the Shuttle thread will perpetually enqueue messages resulting in a
         * potential to run out of memory. This method will dequeue the oldest messages in the
         * queue IFF the size of the queue grows to TWICE the maxPendingMessagesPerContainer
         * set on the container.
         */
        public void handleEnqueuing(final ContainerJobHolder curJobHolder) {
            // offer the job. If the job is queued then ownership of the lifecycle has been successfully passed to the container...
            if(!queue.offer(curJobHolder)) {
                // ... otherwise we need to make sure we mark the job as rejected at the container level.
                curJobHolder.reject(container);
                LOGGER.trace("Failed to be queued to container {}. The queue has {} messages in it",
                    container.container.getClusterId(), queue.size());
            } else if(shedMode) {
                // we need to check the length of the queue and remove the oldest if it's too long
                // and we need to make sure we mark the job as rejected at the container level.
                final int queueSize = queue.size();
                if(queueSize > maxPendingMessagesPerContainerX2) {
                    // TODO: this can reject non-limited. The should be handled using maxNumWaitingLimitedTasks
                    // and stalling the Shuttle threads
                    final ContainerJobHolder rejectedOld = queue.poll();
                    if(rejectedOld != null) {
                        rejectedOld.reject(container);
                        LOGGER.trace("Failed to be queued to container {}. The queue has {} messages in it",
                            container.container.getClusterId(), queueSize);
                    }
                }
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
            final ContainerJobMetadata[] deliveries = message.job.containerData();

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
                    final ContainerJobMetadata jobData = deliveries[i];

                    // container
                    final Container curContainer = jobData.container;
                    // this is a single thread so this should be safe. The Function<> is NOT side effect free. It starts a thread.
                    final ContainerWorker curWorker = containerWorkers.computeIfAbsent(curContainer, x -> new ContainerWorker(jobData));

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
                boolean someWorkDone = false;
                // ========================================================
                // Phase I of this event loop:
                // check the inqueue.
                // ========================================================
                try {
                    final MessageDeliveryJobHolder message = inqueue.poll();

                    if(message != null) {
                        if(LOGGER.isDebugEnabled())
                            occLogger.run();

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
                    final MessageDeliveryJobHolder peeked = deserQueue.peek();
                    if(peeked != null && peeked.areContainersCalculated()) {
                        // we're the only thread reading this queue ...
                        final MessageDeliveryJobHolder message = deserQueue.poll();
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
        logConfig(LOGGER, "Threading Model", OrderedPerContainerThreadingModel.class.getName());
        logConfig(LOGGER, configKey(CONFIG_KEY_MAX_PENDING), getMaxNumberOfQueuedLimitedTasks(), DEFAULT_MAX_PENDING);
        logConfig(LOGGER, configKey(CONFIG_KEY_DESERIALIZATION_THREADS), deserializationThreadCount, DEFAULT_DESERIALIZATION_THREADS);

        shuttleThread = chain(newThread(new Shuttler(), nameSupplier.get() + "-Shuttle"), t -> t.start());
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
    public void submit(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, false, numLimitedX, maxNumWaitingLimitedTasks);
        if(!inqueue.offer(jobh)) {
            jobh.reject();
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .map(c -> c.container)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }

    @Override
    public void submitPrioity(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, false, numLimitedX, maxNumWaitingLimitedTasks);
        if(!inqueue.offerFirst(jobh)) {
            jobh.reject();
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .map(c -> c.container)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }

    @Override
    public void submitLimited(final MessageDeliveryJob job) {
        final MessageDeliveryJobHolder jobh = new MessageDeliveryJobHolder(job, true, numLimitedX, maxNumWaitingLimitedTasks);
        if(!inqueue.offer(jobh)) {
            jobh.reject();// undo, since we failed. Though this should be impossible
            LOGGER.error("Failed to queue message destined for {}",
                Optional.ofNullable(job.containerData())
                    .map(v -> Arrays.stream(v)
                        .map(c -> c.container)
                        .filter(c -> c != null)
                        .map(c -> c.getClusterId())
                        .map(cid -> cid.toString())
                        .collect(Collectors.toList()))
                    .orElse(List.of("null")));
        }
    }
}
