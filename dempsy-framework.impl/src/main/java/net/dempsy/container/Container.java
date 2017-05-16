/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.container;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.Service;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.output.OutputInvoker;
import net.dempsy.router.RoutingStrategy.Inbound;
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
    protected final Logger LOGGER;

    private static final AtomicLong containerNumSequence = new AtomicLong(0L);
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
    protected ClusterStatsCollector statCollector;
    protected Set<String> messageTypes;

    private ExecutorService outputExecutorService = null;
    protected int outputConcurrency = -1;
    private final AtomicLong outputThreadNum = new AtomicLong();

    protected Container(final Logger LOGGER) {
        this.LOGGER = LOGGER;
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

    public Container setInbound(final Inbound inbound) {
        this.inbound = inbound;
        return this;
    }

    @SuppressWarnings("unchecked")
    public Container setMessageProcessor(final MessageProcessorLifecycle<?> prototype) {
        if (prototype == null)
            throw new IllegalArgumentException("The container for cluster(" + clusterId + ") cannot be supplied a null MessageProcessor");

        this.prototype = (MessageProcessorLifecycle<Object>) prototype;

        return this;
    }

    public Container setClusterId(final ClusterId clusterId) {
        if (clusterId == null)
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
        if (evictionScheduler != null)
            evictionScheduler.shutdownNow();

        // the following will close up any output executor that might be running
        stopOutput();

        isRunning.set(false);
        isRunningLazy = false;
    }

    @Override
    public void start(final Infrastructure infra) {
        isRunningLazy = true;
        isRunning.set(true);

        statCollector = infra.getClusterStatsCollector(clusterId);

        validate();

        if (evictionCycleTime != -1)
            startEvictionThread();

        prototype.start(clusterId);

        messageTypes = ((MessageProcessorLifecycle<?>) prototype).messagesTypesHandled();
        if (messageTypes == null || messageTypes.size() == 0)
            throw new ContainerException("The cluster " + clusterId + " appears to have a MessageProcessor with no messageTypes defined.");

        if (outputConcurrency > 1)
            outputExecutorService = Executors.newFixedThreadPool(outputConcurrency,
                    r -> new Thread(r, this.getClass().getSimpleName() + "-Output-" + outputThreadNum.getAndIncrement()));
    }

    @Override
    public void invokeOutput() {
        try (final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted()) {
            outputPass();
        }
    }

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    // this is called directly from tests but shouldn't be accessed otherwise.
    public abstract void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException;

    protected abstract void doevict(EvictCheck check);

    // This method MUST NOT THROW
    protected abstract void outputPass();

    protected ExecutorService getOutputExecutorService() {
        return outputExecutorService;
    }

    @Override
    public void setOutputConcurrency(final int concurrency) {
        outputConcurrency = concurrency;
    }

    private void stopOutput() {
        if (outputExecutorService != null)
            outputExecutorService.shutdown();

        outputExecutorService = null;
    }

    public void setEvictionCycle(final long evictionCycleTime, final TimeUnit timeUnit) {
        this.evictionCycleTime = evictionCycleTime;
        this.evictionTimeUnit = timeUnit;
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
    private ScheduledExecutorService evictionScheduler;

    private final static AtomicLong evictionThreadNumber = new AtomicLong(0);

    private void startEvictionThread() {
        if (0 == evictionCycleTime || null == evictionTimeUnit) {
            LOGGER.warn("Eviction Thread cannot start with zero cycle time or null TimeUnit {} {}", evictionCycleTime, evictionTimeUnit);
            return;
        }

        if (prototype != null && prototype.isEvictionSupported()) {
            evictionScheduler = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, this.getClass().getSimpleName() + "-Eviction-" + evictionThreadNumber.getAndIncrement()));

            evictionScheduler.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    evict();
                }
            }, evictionCycleTime, evictionCycleTime, evictionTimeUnit);
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

                if (shrink) {
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
                    } catch (final RuntimeException rte) {
                        LOGGER.error("Failed on eviction", rte);
                    }
                }

                if (grow) {
                    // TODO: the grow only affect pre-instantiation capable containers
                    // no grow yet.
                }

                grow = shrink = false;
            } catch (final RuntimeException exception) {
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

        if (less) {
            // we need to run a special eviction pass.
            synchronized (changer) { // we only want to do this one at a time.
                keyspaceChangeSwitch.preemptWorkerAndWait(); // if it's already running the stop it so we can restart it.

                changer.inbound = inbound;

                // we don't want to set either to false in case preempting a previous
                // change created an incomplete state. The state will be redone here and
                // only when a complete, uninterrupted pass finishes will the states
                // of grow and shrink be reset (see the last line of the run() method
                // in the KeyspaceChanger.
                if (more)
                    changer.grow = true;
                if (less)
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
        if (clusterId == null)
            throw new IllegalStateException("The container must have a cluster id");

        if (prototype == null)
            throw new IllegalStateException("The container for \"" + clusterId + "\" cannot be supplied a null MessageProcessor");

        if (dispatcher == null)
            throw new IllegalStateException("The container for cluster \"" + clusterId + "\" never had the dispatcher set on it.");

        if (statCollector == null)
            throw new IllegalStateException("The container must have a " + ClusterStatsCollector.class.getSimpleName() + " id");
    }

}
