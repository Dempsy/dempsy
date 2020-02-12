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

package net.dempsy.container.locking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.container.Container;
import net.dempsy.container.ContainerException;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.util.SafeString;

/**
 * <p>
 * The {@link LockingContainer} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 * <p>
 * Behavior:
 * </p>
 * <ul>
 * <li>Can set maxPendingMessagesPerContainer</li>
 * <li>Doesn't internally queue messages</li>
 * <li>Doesn't handle bulk processing</li>
 * <li>Guarantee's order in submission of outgoing responses</li>
 * <li>Lower performing than other options.</li>
 * <li>Most deterministic behavior</li>
 * </ul>
 *
 */
public class LockingContainer extends Container {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockingContainer.class);
    // This is a bad idea but only used to gate trace logging the invocation.

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    private final ConcurrentHashMap<Object, InstanceWrapper> instances = new ConcurrentHashMap<>();

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final AtomicInteger numBeingWorked = new AtomicInteger(0);

    public LockingContainer() {
        super(LOGGER);
    }

    // ----------------------------------------------------------------------------
    // Configuration
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Monitoring / Management
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Operation
    // ----------------------------------------------------------------------------

    @Override
    public void start(final Infrastructure infra) {
        super.start(infra);
        isReady.set(true);
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    // ----------------------------------------------------------------------------
    // Monitoring and Management
    // ----------------------------------------------------------------------------

    /**
     * Returns the number of message processors controlled by this manager.
     */
    @Override
    public int getProcessorCount() {
        return instances.size();
    }

    @Override
    public int getMessageWorkingCount() {
        return numBeingWorked.get();
    }

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------
    @Override
    public Object getMp(final Object key) {
        final InstanceWrapper iw = instances.get(key);
        return iw == null ? null : iw.instance;
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    protected class InstanceWrapper {
        private final Object instance;
        private final Semaphore lock = new Semaphore(1, true); // basically a mutex
        private boolean evicted = false;

        /**
         * DO NOT CALL THIS WITH NULL OR THE LOCKING LOGIC WON'T WORK
         */
        public InstanceWrapper(final Object o) {
            this.instance = o;
        }

        /**
         * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
         *
         * @return the instance if the lock was acquired. null otherwise.
         */
        public Object getExclusive() {
            boolean gotLock = false;
            while(!gotLock) {
                try {
                    lock.acquire();
                    gotLock = true;
                } catch(final InterruptedException e) {
                    if(!isRunning.get()) {
                        throw new DempsyException("Stopped");
                    }
                }
            }

            // if we got here we have the lock
            return instance;
        }

        /**
         * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK. MAKE SURE YOU OWN THE LOCK IF YOU UNLOCK IT.
         */
        public void releaseLock() {
            lock.release();
        }

        public boolean tryLock() {
            return lock.tryAcquire();
        }

        // /**
        // * This will set the instance reference to null. MAKE SURE
        // * YOU OWN THE LOCK BEFORE CALLING.
        // */
        // public void markPassivated() { instance = null; }
        //
        // /**
        // * This will tell you if the instance reference is null. MAKE SURE
        // * YOU OWN THE LOCK BEFORE CALLING.
        // */
        // public boolean isPassivated() { return instance == null; }

        /**
         * This will prevent further operations on this instance. MAKE SURE YOU OWN THE LOCK BEFORE CALLING.
         */
        public void markEvicted() {
            evicted = true;
        }

        /**
         * Flag to indicate this instance has been evicted and no further operations should be enacted. THIS SHOULDN'T
         * BE CALLED WITHOUT HOLDING THE LOCK.
         */
        public boolean isEvicted() {
            return evicted;
        }

        // ----------------------------------------------------------------------------
        // Test access
        // ----------------------------------------------------------------------------
        protected Object getInstance() {
            return instance;
        }
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage keyedMessage, final boolean youOwnMessage) throws IllegalArgumentException, ContainerException {
        if(!isRunningLazy) {
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("Dispatch called on stopped container");
            statCollector.messageFailed(1);
            if(youOwnMessage)
                disposition.dispose(keyedMessage.message);
            return;
        }

        if(keyedMessage == null)
            return; // No. We didn't process the null message

        if(keyedMessage.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch a null message.");

        final Object actualMessage = youOwnMessage ? keyedMessage.message : disposition.replicate(keyedMessage.message);
        final Object messageKey = keyedMessage.key;

        if(messageKey == null) {
            disposition.dispose(actualMessage);
            throw new ContainerException("Message " + objectDescription(actualMessage) + " contains no key.");
        }

        if(!inbound.doesMessageKeyBelongToNode(messageKey)) {
            disposition.dispose(actualMessage);
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("Message with key " + SafeString.objectDescription(messageKey) + " sent to wrong container. ");
            statCollector.messageFailed(1);
            return;
        }

        boolean evictedAndBlocking;

        try {
            numBeingWorked.incrementAndGet();

            do {
                evictedAndBlocking = false;

                final InstanceWrapper wrapper = getInstanceForKey(messageKey);

                // wrapper will be null if the activate returns 'false'
                if(wrapper != null) {
                    final Object instance = wrapper.getExclusive();
                    if(instance != null) { // null indicates we didn't get the lock
                        try {
                            if(wrapper.isEvicted()) {
                                // if we're not blocking then we need to just return a failure. Otherwise we want to try
                                // again because eventually the current Mp will be passivated and removed from the container
                                // and a subsequent call to getInstanceForDispatch will create a new one.
                                Thread.yield();
                                evictedAndBlocking = true; // we're going to try again.
                            } else {
                                invokeOperationAndHandleDispose(wrapper.getInstance(), Operation.handle, new KeyedMessage(messageKey, actualMessage));
                            }
                        } finally {
                            wrapper.releaseLock();
                        }
                    } else { // ... we didn't get the lock
                        if(LOGGER.isTraceEnabled())
                            LOGGER.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
                        statCollector.messageCollision(actualMessage);
                        disposition.dispose(actualMessage);
                    }
                } else {
                    // if we got here then the activate on the Mp explicitly returned 'false'
                    if(LOGGER.isDebugEnabled())
                        LOGGER.debug("the container for " + clusterId + " failed to activate the Mp for " + SafeString.valueOf(prototype));
                    disposition.dispose(actualMessage);
                    // we consider this "processed"
                    break; // leave the do/while loop
                }
            } while(evictedAndBlocking);

        } finally {
            numBeingWorked.decrementAndGet();
        }
    }

    @Override
    protected void doevict(final EvictCheck check) {
        if(!check.isGenerallyEvitable() || !isRunning.get())
            return;

        try (final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted()) {

            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Map<Object, InstanceWrapper> instancesToEvict = new HashMap<>(instances.size() + 10);
            instancesToEvict.putAll(instances);

            while(instancesToEvict.size() > 0 && instances.size() > 0 && isRunning.get() && !check.shouldStopEvicting()) {
                // store off anything that passes for later removal. This is to avoid a
                // ConcurrentModificationException.
                final Set<Object> keysProcessed = new HashSet<>();

                for(final Map.Entry<Object, InstanceWrapper> entry: instancesToEvict.entrySet()) {
                    if(check.shouldStopEvicting())
                        break;

                    final Object key = entry.getKey();
                    final InstanceWrapper wrapper = entry.getValue();
                    boolean gotLock = false;
                    try {
                        gotLock = wrapper.tryLock();
                        if(gotLock) {

                            // since we got here we're done with this instance,
                            // so add it's key to the list of keys we plan don't
                            // need to return to.
                            keysProcessed.add(key);

                            boolean removeInstance = false;

                            try {
                                if(check.shouldEvict(key, wrapper.instance)) {
                                    removeInstance = true;
                                    wrapper.markEvicted();
                                    prototype.passivate(wrapper.getInstance());
                                    // wrapper.markPassivated();
                                }
                            } catch(final Throwable e) {
                                Object instance = null;
                                try {
                                    instance = wrapper.getInstance();
                                } catch(final Throwable th) {} // not sure why this would ever happen
                                LOGGER.warn("Checking the eviction status/passivating of the Mp "
                                    + SafeString.objectDescription(instance == null ? wrapper : instance) +
                                    " resulted in an exception.", e);
                            }

                            // even if passivate throws an exception, if the eviction check returned 'true' then
                            // we need to remove the instance.
                            if(removeInstance) {
                                if(LOGGER.isTraceEnabled())
                                    LOGGER.trace("Evicting Mp with key " + SafeString.objectDescription(key) + " from " + clusterId.toString());
                                instances.remove(key);
                                statCollector.messageProcessorDeleted(key);
                            }
                        }
                    } finally {
                        if(gotLock)
                            wrapper.releaseLock();
                    }

                }

                // now clean up everything we managed to get hold of
                for(final Object key: keysProcessed)
                    instancesToEvict.remove(key);

            }
        }
    }

    // This method MUST NOT THROW
    @Override
    protected void outputPass() {
        if(!prototype.isOutputSupported())
            return;

        // take a snapshot of the current container state.
        final LinkedList<InstanceWrapper> toOutput = new LinkedList<>(instances.values());

        Executor executorService = null;
        Semaphore taskLock = null;
        executorService = super.getOutputExecutorService();
        if(executorService != null)
            taskLock = new Semaphore(outputConcurrency);

        // This keeps track of the number of concurrently running
        // output tasks so that this method can wait until they're
        // all done to return.
        //
        // It's also used as a condition variable signaling on its
        // own state changes.
        final AtomicLong numExecutingOutputs = new AtomicLong(0);

        // keep going until all of the outputs have been invoked
        while(toOutput.size() > 0 && isRunning.get()) {
            for(final Iterator<InstanceWrapper> iter = toOutput.iterator(); iter.hasNext();) {
                final InstanceWrapper wrapper = iter.next();
                boolean gotLock = false;

                gotLock = wrapper.tryLock();

                if(gotLock) {
                    // If we've been evicted then we're on our way out
                    // so don't do anything else with this.
                    if(wrapper.isEvicted()) {
                        iter.remove();
                        wrapper.releaseLock();
                        continue;
                    }

                    final Object instance = wrapper.getInstance(); // only called while holding the lock
                    final Semaphore taskSepaphore = taskLock;

                    // This task will release the wrapper's lock.
                    final Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if(isRunning.get() && !wrapper.isEvicted())
                                    invokeOperationAndHandleDispose(instance, Operation.output, null);
                            } finally {
                                wrapper.releaseLock();

                                // this signals that we're done.
                                synchronized(numExecutingOutputs) {
                                    numExecutingOutputs.decrementAndGet();
                                    numExecutingOutputs.notifyAll();
                                }
                                if(taskSepaphore != null)
                                    taskSepaphore.release();
                            }
                        }
                    };

                    synchronized(numExecutingOutputs) {
                        numExecutingOutputs.incrementAndGet();
                    }

                    if(executorService != null) {
                        try {
                            taskSepaphore.acquire();
                            executorService.execute(task);
                        } catch(final RejectedExecutionException e) {
                            // this may happen because of a race condition between the
                            taskSepaphore.release();
                            wrapper.releaseLock(); // we never got into the run so we need to release the lock
                        } catch(final InterruptedException e) {
                            // this can happen while blocked in the semaphore.acquire.
                            // if we're no longer running we should just get out
                            // of here.
                            //
                            // Not releasing the taskSepaphore assumes the acquire never executed.
                            // if (since) the acquire never executed we also need to release the
                            // wrapper lock or that Mp will never be usable again.
                            wrapper.releaseLock(); // we never got into the run so we need to release the lock
                        }
                    } else
                        task.run();

                    iter.remove();
                } // end if we got the lock
            } // end loop over every Mp
        } // end while there are still Mps that haven't had output invoked.

        // =======================================================
        // now make sure all of the running tasks have completed
        synchronized(numExecutingOutputs) {
            while(numExecutingOutputs.get() > 0) {
                try {
                    numExecutingOutputs.wait(300);
                    // If the executor gets shut down we wont know, so we need to check once in a while
                    if(!isRunning.get())
                        break;
                } catch(final InterruptedException e) {
                    // if we were interupted for a shutdown then just stop
                    // waiting for all of the threads to finish
                    if(!isRunning.get())
                        break;
                    // otherwise continue checking.
                }
            }
        }
        // =======================================================
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    ConcurrentHashMap<Object, Boolean> keysBeingWorked = new ConcurrentHashMap<>();

    /**
     * This is required to return non null or throw a ContainerException
     */
    InstanceWrapper getInstanceForKey(final Object key) throws ContainerException {
        // common case has "no" contention
        InstanceWrapper wrapper = instances.get(key);
        if(wrapper != null)
            return wrapper;

        // otherwise we will be working to get one.
        final Boolean tmplock = Boolean.TRUE;
        Boolean lock = keysBeingWorked.putIfAbsent(key, tmplock);
        if(lock == null)
            lock = tmplock;

        // otherwise we'll do an atomic check-and-update
        synchronized(lock) {
            wrapper = instances.get(key); // double checked lock?????
            if(wrapper != null)
                return wrapper;

            Object instance = null;
            try {
                instance = prototype.newInstance();
            } catch(final DempsyException e) {
                if(e.userCaused()) {
                    LOGGER.warn("The message processor prototype " + SafeString.valueOf(prototype)
                        + " threw an exception when trying to create a new message processor for they key " + SafeString.objectDescription(key));
                    statCollector.messageFailed(1);
                    instance = null;
                } else
                    throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                        SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                        " because the clone method threw an exception.", e);
            } catch(final RuntimeException e) {
                throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                    SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                    " because the clone invocation resulted in an unknown exception.", e);
            }

            // activate
            boolean activateSuccessful = false;
            try {
                if(instance != null) {
                    if(LOGGER.isTraceEnabled())
                        LOGGER.trace("the container for " + clusterId + " is activating instance " + String.valueOf(instance)
                            + " via " + SafeString.valueOf(prototype) + " for " + SafeString.valueOf(key));
                    prototype.activate(instance, key);
                    activateSuccessful = true;
                }
            } catch(final DempsyException e) {
                if(e.userCaused()) {
                    if(LOGGER.isDebugEnabled())
                        LOGGER.warn("The message processor " + SafeString.objectDescription(instance) + " activate call threw an exception.", e.userCause);
                    else
                        LOGGER.warn("The message processor " + SafeString.objectDescription(instance) + " activate call threw an exception.");
                    statCollector.messageFailed(1);
                } else
                    throw new ContainerException(
                        "the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype)
                            + ". Is the active method accessible - the class is public and the method is public?",
                        e);
            } catch(final RuntimeException e) {
                throw new ContainerException(
                    "the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +
                        " because of an unknown exception.",
                    e);
            }

            if(activateSuccessful) {
                // we only want to create a wrapper and place the instance into the container
                // if the instance activated correctly. If we got here then the above try block
                // must have been successful.
                wrapper = new InstanceWrapper(instance); // null check above.
                instances.put(key, wrapper); // once it goes into the map, we can remove it from the 'being worked' set
                keysBeingWorked.remove(key); // remove it from the keysBeingWorked since any subsequent call will get
                // the newly added one.
                statCollector.messageProcessorCreated(key);
            }
            return wrapper;
        }
    }

}
