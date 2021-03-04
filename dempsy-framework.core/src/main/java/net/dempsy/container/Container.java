/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.container;

import static net.dempsy.config.ConfigLogger.logConfig;
import static net.dempsy.util.SafeString.objectDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.Service;
import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.DummyMessageResourceManager;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.messages.MessageResourceManager;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.output.OutputInvoker;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.threading.QuartzHelper;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.util.OccasionalRunnable;
import net.dempsy.util.QuietCloseable;
import net.dempsy.util.SafeString;
import net.dempsy.util.executor.RunningEventSwitch;

/**
 * <p>
 * The {@link Container} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 *
 * The container is simple in that it does no thread management. When it's called it assumes that the transport
 * has provided the thread that's needed
 */
public abstract class Container implements Service, KeyspaceChangeListener, OutputInvoker {
    private static final Logger SLOGGER = LoggerFactory.getLogger(Container.class);
    protected final Logger LOGGER;

    protected final boolean traceEnabled;

    private static final AtomicLong containerNumSequence = new AtomicLong(0L);

    private static final long DEFAULT_LOG_QUEUE_LEN_MESSAGE_COUNT = 1024;
    private static final String CONFIG_KEY_LOG_QUEUE_LEN_MESSAGE_COUNT = "log_queue_len_message_count";

    protected long containerNum = containerNumSequence.getAndIncrement();

    protected Dispatcher dispatcher;
    protected Inbound inbound;

    // The ClusterId is set for the sake of error messages.
    protected ClusterId clusterId;

    protected long evictionCycleTime = -1;
    protected TimeUnit evictionTimeUnit = null;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);
    protected boolean isRunningLazy = false;

    protected MessageProcessorLifecycle<Object> prototype;
    protected MessageResourceManager disposition = null;
    protected boolean hasDisposition = false;

    protected ClusterStatsCollector statCollector;
    protected Set<String> messageTypes;

    private ExecutorService outputExecutorService = null;
    protected int outputConcurrency = -1;
    private final AtomicLong outputThreadNum = new AtomicLong();

    protected AtomicInteger numPending = new AtomicInteger(0);
    protected int maxPendingMessagesPerContainer = Cluster.DEFAULT_MAX_PENDING_MESSAGES_PER_CONTAINER;

    protected long logQueueMessageCount = DEFAULT_LOG_QUEUE_LEN_MESSAGE_COUNT;
    protected Runnable occLogger = () -> {};

    protected Container(final Logger LOGGER) {
        this.LOGGER = LOGGER;
        traceEnabled = LOGGER.isTraceEnabled();
    }

    public enum Operation {
        handle, output, bulk
    }

    // ----------------------------------------------------------------------------
    // Configuration
    // ----------------------------------------------------------------------------

    public Container setDispatcher(final Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        return this;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public Container setMaxPendingMessagesPerContainer(final int maxPendingMessagesPerContainer) {
        this.maxPendingMessagesPerContainer = maxPendingMessagesPerContainer;
        return this;
    }

    public int getMaxPendingMessagesPerContainer() {
        return maxPendingMessagesPerContainer;
    }

    public Container setInbound(final Inbound inbound) {
        this.inbound = inbound;
        return this;
    }

    @SuppressWarnings("unchecked")
    public Container setMessageProcessor(final MessageProcessorLifecycle<?> prototype) {
        if(prototype == null)
            throw new IllegalArgumentException("The container for cluster(" + clusterId + ") cannot be supplied a null MessageProcessor");

        this.prototype = (MessageProcessorLifecycle<Object>)prototype;
        final MessageResourceManager resourceManager = this.prototype.manager();
        this.hasDisposition = resourceManager != null;
        this.disposition = hasDisposition ? resourceManager : new DummyMessageResourceManager();
        return this;
    }

    /**
     * Always called from the Dempsy infrastructure (NodeManager) prior to calling start().
     */
    public Container setClusterId(final ClusterId clusterId) {
        if(clusterId == null)
            throw new IllegalArgumentException("The container must have a cluster id");

        this.clusterId = clusterId;

        return this;
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    // ----------------------------------------------------------------------------
    // Monitoring / Management
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Operation
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Monitoring and Management
    // ----------------------------------------------------------------------------

    /**
     * Returns the number of message processors controlled by this manager.
     */
    public abstract int getProcessorCount();

    /**
     * The number of messages the container is currently working on. This may not match the
     * stats collector's numberOfInFlight messages which is a measurement of how many messages
     * are currently being handled by MPs. This represents how many messages have been given
     * dispatched to the container that are yet being processed. It includes internal queuing
     * in the container as well as messages actually in Mp handlers.
     */
    public abstract int getMessageWorkingCount();

    @Override
    public void stop() {
        if(evictionScheduler != null) {
            try {
                evictionScheduler.shutdown(false);
            } catch(final SchedulerException se) {
                LOGGER.error("Failed to shut down the scheduler for the eviction check cycle.");
            }
        }

        // the following will close up any output executor that might be running
        stopOutput();

        isRunning.set(false);
        isRunningLazy = false;
    }

    @Override
    public void start(final Infrastructure infra) {
        if(traceEnabled)
            LOGGER.trace("Container {} maxPendingMessagesPerContainer:", clusterId, maxPendingMessagesPerContainer);

        final Map<String, String> configuration = infra.getConfiguration();
        logQueueMessageCount = Long
            .parseLong(getConfigValue(configuration, CONFIG_KEY_LOG_QUEUE_LEN_MESSAGE_COUNT, "" + DEFAULT_LOG_QUEUE_LEN_MESSAGE_COUNT));
        if(LOGGER.isTraceEnabled()) { // this configuration is ignored if the log level isn't debug
            logConfig(LOGGER, configKey(CONFIG_KEY_LOG_QUEUE_LEN_MESSAGE_COUNT), logQueueMessageCount, DEFAULT_LOG_QUEUE_LEN_MESSAGE_COUNT);
            occLogger = OccasionalRunnable.staticOccasionalRunnable(logQueueMessageCount,
                () -> LOGGER.trace("Total messages pending on " + this.getClass().getSimpleName() + " container for {}: {}", clusterId,
                    getMessageWorkingCount()));
        }

        isRunningLazy = true;
        isRunning.set(true);

        statCollector = infra.getClusterStatsCollector(clusterId);

        validate();

        if(evictionCycleTime != -1) {
            try {
                startEvictionThread();
            } catch(final SchedulerException e) {
                throw new DempsyException("Failed to start eviction checking scheduler", e, false);
            }
        }

        prototype.start(clusterId);

        messageTypes = ((MessageProcessorLifecycle<?>)prototype).messagesTypesHandled();
        if(messageTypes == null || messageTypes.size() == 0)
            throw new ContainerException("The cluster " + clusterId + " appears to have a MessageProcessor with no messageTypes defined.");

        if(outputConcurrency > 1)
            outputExecutorService = Executors.newFixedThreadPool(outputConcurrency,
                r -> new Thread(r, this.getClass().getSimpleName() + "-Output-" + outputThreadNum.getAndIncrement()));

        // check the container support for bulk
        if(prototype.isBulkDeliverySupported() && !containerSupportsBulkProcessing())
            LOGGER.warn("The MessageProcessor for {} supports bulk message processing but the container does not.", clusterId);
        else if(!prototype.isBulkDeliverySupported() && containerSupportsBulkProcessing())
            LOGGER.info("The container {} supports bulk processing but the message processor for {} does not.", this.getClass().getSimpleName(), clusterId);
    }

    @Override
    public void invokeOutput() {
        try(final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted()) {
            outputPass();
        }
    }

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    // The Container message processing happens in 2 phases.
    // Phase 1) Generate the payload from a newly arrived (or fed-backed) message. This will then be queued with the
    // ThreadManager.
    //
    // Phase 2) When the payload is delivered in the working thread it can be dispatched to the container.
    //
    // The details of the ContainerPayload will be specific to the container implementation but will likely
    // do things like throw away messages when the number pending to that container is too large.
    public static interface ContainerSpecific {
        void messageBeingDiscarded();
    }

    // Called before being submitted to the ThreadingModel. It allows independent management
    // of the portion of the ThreadingModel queue that's dedicated to queuing messages internally
    // to a container. Messages that are coming in from the outside of the node will have 'justArrived'
    // and therefore don't count against the maxPendingMessagesPerContainer. They count against
    // the nodes' queue.
    public ContainerSpecific prepareMessage(final RoutedMessage km, final boolean justArrived) {
        if(maxPendingMessagesPerContainer < 0)
            return null;
        if(justArrived)
            return null; // there's no bookeeping if the message just arrived.

        numPending.incrementAndGet();
        if(traceEnabled)
            LOGGER.trace("prepareMessages: Pending messages on {} container is: ", clusterId, numPending);

        return new ContainerSpecificInternal();
    }

    public void dispatch(final KeyedMessage message, final ContainerSpecific cs, final boolean justArrived)
        throws IllegalArgumentException, ContainerException {

        if(cs != null) {
            final int num = numPending.decrementAndGet(); // dec first.

            if(num > maxPendingMessagesPerContainer) { // we just vent the message.
                statCollector.messageDiscarded(message);
                return;
            }
        }

        if(traceEnabled)
            LOGGER.trace("dispatch: Pending messages on {} container is: ", clusterId, numPending);

        occLogger.run();

        dispatch(message, justArrived);
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    //
    // implementations MUST handle the disposition
    public abstract void dispatch(final KeyedMessage message, boolean youOwnMessage) throws IllegalArgumentException, ContainerException;

    protected abstract void doevict(EvictCheck check);

    // This method MUST NOT THROW
    protected abstract void outputPass();

    /**
     * This should ONLY be used for testing. It will retrieve the current Mp by key
     * if it exists. No processing stops and the bookeeping of the container is not affected.
     * The Mp can be removed from the container at any time.
     */
    public abstract Object getMp(Object key);

    /**
     * This needs to be overloaded to tell the framework whether or not the container implementation
     * supports internal queuing. This is because some threading models are incompatible with containers
     * that internally queue messages.
     */
    public abstract boolean containerInternallyQueuesMessages();

    public abstract boolean containerSupportsBulkProcessing();

    public void setEvictionCycle(final long evictionCycleTime, final TimeUnit timeUnit) {
        this.evictionCycleTime = evictionCycleTime;
        this.evictionTimeUnit = timeUnit;
    }

    protected ExecutorService getOutputExecutorService() {
        return outputExecutorService;
    }

    @Override
    public void setOutputConcurrency(final int concurrency) {
        outputConcurrency = concurrency;
    }

    private class ContainerSpecificInternal implements ContainerSpecific {
        @Override
        public void messageBeingDiscarded() {
            numPending.decrementAndGet();
        }
    }

    private void stopOutput() {
        if(outputExecutorService != null)
            outputExecutorService.shutdown();

        outputExecutorService = null;
    }

    // =======================================================================================
    // Handle eviction capabilities
    protected interface EvictCheck {
        boolean isGenerallyEvitable();

        boolean shouldEvict(Object key, Object instance);

        boolean shouldStopEvicting();
    }

    public void evict() {
        doevict(new EvictCheck() {

            @Override
            public boolean shouldStopEvicting() {
                return false;
            }

            @Override
            public boolean shouldEvict(final Object key, final Object instance) {
                return prototype.invokeEvictable(instance);
            }

            @Override
            public boolean isGenerallyEvitable() {
                return prototype.isEvictionSupported();
            }
        });
    }

    // Scheduler to handle eviction thread.
    private Scheduler evictionScheduler = null;

    public static final String EVICTION_CHECK_JOB_NAME = "evictionCheckInvoker";

    public static class EvictionCheckJob implements Job {
        @Override
        public void execute(final JobExecutionContext context) throws JobExecutionException {

            final JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            final Container evictInvoker = (Container)dataMap.get(EVICTION_CHECK_JOB_NAME);

            if(evictInvoker != null) {
                // execute MP's output method
                evictInvoker.evict();
            } else {
                SLOGGER.warn("evition check invoker is NULL");
            }
        }

    }

    private void startEvictionThread() throws SchedulerException {
        if(0 == evictionCycleTime || null == evictionTimeUnit) {
            LOGGER.warn("Eviction Thread cannot start with zero cycle time or null TimeUnit {} {}", evictionCycleTime, evictionTimeUnit);
            return;
        }

        if(prototype != null && prototype.isEvictionSupported()) {
            final JobBuilder jobBuilder = JobBuilder.newJob(EvictionCheckJob.class);
            final JobDetail jobDetail = jobBuilder.build();
            jobDetail.getJobDataMap().put(EVICTION_CHECK_JOB_NAME, this);

            final Trigger trigger = QuartzHelper.getSimpleTrigger(evictionTimeUnit, (int)evictionCycleTime, true);
            evictionScheduler = StdSchedulerFactory.getDefaultScheduler();
            evictionScheduler.scheduleJob(jobDetail, trigger);
            evictionScheduler.start();
        }
    }

    // =======================================================================================
    // Manage keyspace change events.
    private final RunningEventSwitch keyspaceChangeSwitch = new RunningEventSwitch(isRunning);

    public class KeyspaceChanger implements Runnable {
        boolean grow = false;
        boolean shrink = false;
        Inbound inbound;

        @Override
        public void run() {
            try {
                // Notify that we're running
                keyspaceChangeSwitch.workerRunning();

                if(shrink) {
                    LOGGER.trace("Evicting Mps due to keyspace shrinkage.");
                    try {
                        // First do the contract by evicting all
                        doevict(new EvictCheck() {
                            // we shouldEvict if the message key no longer belongs as
                            // part of this container.
                            // strategyInbound can't be null if we're here since this was invoked
                            // indirectly from it. So here we don't need to check for null.
                            @Override
                            public boolean shouldEvict(final Object key, final Object instance) {
                                return !inbound.doesMessageKeyBelongToNode(key);
                            }

                            // In this case, it's evictable.
                            @Override
                            public boolean isGenerallyEvitable() {
                                return true;
                            }

                            @Override
                            public boolean shouldStopEvicting() {
                                return keyspaceChangeSwitch.wasPreempted();
                            }
                        });
                    } catch(final RuntimeException rte) {
                        LOGGER.error("Failed on eviction", rte);
                    }
                }

                if(grow) {
                    // TODO: the grow only affect pre-instantiation capable containers
                    // no grow yet.
                }

                grow = shrink = false;
            } catch(final RuntimeException exception) {
                LOGGER.error("Failed to shrink the KeySpace.", exception);
            } finally {
                keyspaceChangeSwitch.workerFinished();
            }
        }
    }

    private final KeyspaceChanger changer = new KeyspaceChanger();
    private static AtomicLong keyspaceChangeThreadNum = new AtomicLong(0L);

    @Override
    public void keyspaceChanged(final boolean less, final boolean more) {

        if(less) {
            // we need to run a special eviction pass.
            synchronized(changer) { // we only want to do this one at a time.
                keyspaceChangeSwitch.preemptWorkerAndWait(); // if it's already running the stop it so we can restart
                                                             // it.

                changer.inbound = inbound;

                // we don't want to set either to false in case preempting a previous
                // change created an incomplete state. The state will be redone here and
                // only when a complete, uninterrupted pass finishes will the states
                // of grow and shrink be reset (see the last line of the run() method
                // in the KeyspaceChanger.
                if(more)
                    changer.grow = true;
                if(less)
                    changer.shrink = true;

                final Thread t = new Thread(changer, clusterId.toString() + "-Keyspace Change Thread-" + keyspaceChangeThreadNum.getAndIncrement());
                t.setDaemon(true);
                t.start();

                keyspaceChangeSwitch.waitForWorkerToStart();
            }
        }
    }
    // =======================================================================================

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    protected void validate() {
        if(clusterId == null)
            throw new IllegalStateException("The container must have a cluster id");

        if(prototype == null)
            throw new IllegalStateException("The container for \"" + clusterId + "\" cannot be supplied a null MessageProcessor");

        if(dispatcher == null)
            throw new IllegalStateException("The container for cluster \"" + clusterId + "\" never had the dispatcher set on it.");

        if(statCollector == null)
            throw new IllegalStateException("The container must have a " + ClusterStatsCollector.class.getSimpleName() + " id");
    }

    protected static class InvocationResultsCloser implements QuietCloseable {
        public final MessageResourceManager disposition;
        public final boolean hasDisposition;

        private final List<KeyedMessageWithType> toFree = new ArrayList<>();

        public InvocationResultsCloser(final MessageResourceManager disposition) {
            this.disposition = disposition;
            this.hasDisposition = disposition != null;
        }

        private KeyedMessage add(final KeyedMessageWithType km) {
            if(hasDisposition)
                toFree.add(km);
            return km;
        }

        public List<KeyedMessageWithType> addAll(final List<KeyedMessageWithType> vs) {
            if(hasDisposition && vs != null)
                vs.forEach(v -> add(v));
            return vs;
        }

        @Override
        public void close() {
            if(hasDisposition)
                toFree.forEach(v -> disposition.dispose(v.message));
        }

        public void clear() {
            toFree.clear();
        }

    }

    /**
     * helper method to invoke an operation (handle a message or run output) handling all of hte exceptions and
     * forwarding any results.
     */
    protected void invokeOperationAndHandleDispose(final Object instance, final Operation op, final KeyedMessage message) {
        try {
            if(instance != null) { // possibly passivated ...
                try(InvocationResultsCloser resultsDisposerCloser = new InvocationResultsCloser(disposition);) {
                    final List<KeyedMessageWithType> result = invokeGuts(resultsDisposerCloser, instance, op, message, null, 1);
                    if(result != null) {
                        try {
                            dispatcher.dispatch(result, hasDisposition ? disposition : null);
                        } catch(final Exception de) {
                            LOGGER.warn("Failed on subsequent dispatch of " + result + ": " + de.getLocalizedMessage());
                        }
                    }
                }
            }
        } finally {
            if(message != null)
                disposition.dispose(message.message);
        }
    }

    protected void invokeBulkHandleAndHandleDispose(final Object instance, final List<KeyedMessage> messages) {
        try {
            if(instance != null) { // possibly passivated ...
                try(InvocationResultsCloser resultsCloser = new InvocationResultsCloser(disposition);) {
                    final List<KeyedMessageWithType> result = invokeGuts(resultsCloser, instance, Operation.bulk, null, messages, messages.size());
                    if(result != null) {
                        try {
                            dispatcher.dispatch(result, hasDisposition ? disposition : null);
                        } catch(final Exception de) {
                            LOGGER.warn("Failed on subsequent dispatch of " + result + ": " + de.getLocalizedMessage());
                        }
                    }
                }
            }
        } finally {
            if(messages != null)
                messages.forEach(m -> disposition.dispose(m.message));
        }
    }

    protected List<KeyedMessageWithType> invokeOperationAndHandleDisposeAndReturn(final InvocationResultsCloser resultsCloser, final Object instance,
        final Operation op, final KeyedMessage message) {
        try {
            return (instance != null) ? invokeGuts(resultsCloser, instance, op, message, null, 1) : null;
        } finally {
            if(message != null)
                disposition.dispose(message.message);
        }
    }

    private List<KeyedMessageWithType> invokeGuts(final InvocationResultsCloser resultsCloser, final Object instance, final Operation op,
        final KeyedMessage message, final List<KeyedMessage> bulk, final int numMessages) {
        List<KeyedMessageWithType> result;
        try {
            if(traceEnabled)
                LOGGER.trace("invoking \"{}\" for {}", SafeString.valueOf(instance), message);
            statCollector.messageDispatched(numMessages);
            result = resultsCloser.addAll(op == Operation.handle ? prototype.invoke(instance, message)
                : (op == Operation.bulk ? prototype.invokeBulk(instance, bulk) : prototype.invokeOutput(instance)));
            statCollector.messageProcessed(numMessages);
        } catch(final ContainerException e) {
            result = null;
            LOGGER.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " +
                SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + objectDescription(message)) : ""), e);
            statCollector.messageFailed(numMessages);
        }
        // this is an exception thrown as a result of the reflected call having an illegal argument.
        // This should actually be impossible since the container itself manages the calling.
        catch(final IllegalArgumentException e) {
            result = null;
            LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?",
                e);
            statCollector.messageFailed(numMessages);
        }
        // The app threw an exception.
        catch(final DempsyException e) {
            result = null;
            LOGGER.warn("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                " because an exception was thrown by the Message Processeor itself", e);
            statCollector.messageFailed(numMessages);
        }
        // RuntimeExceptions book-keeping
        catch(final RuntimeException e) {
            result = null;
            LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                " due to an unknown exception.", e);
            statCollector.messageFailed(numMessages);

            if(op == Operation.handle || op == Operation.bulk)
                throw e;
        }
        return result;
    }
}
