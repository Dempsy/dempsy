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

package net.dempsy.container.altnonlocking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.container.Container;
import net.dempsy.container.ContainerException;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.util.SafeString;
import net.dempsy.util.StupidHashMap;

/**
 * <p>
 * The {@link NonLockingAltContainer} manages the lifecycle of message processors for the node that it's instantiated
 * in.
 * </p>
 *
 * <p>
 * Behavior:
 * </p>
 * <ul>
 * <li>Can't set maxPendingMessagesPerContainer</li>
 * <li>Internally queues messages (unlimited queue!)</li>
 * <li>DOESN'T handle bulk processing</li>
 * <li>Guarantee's order in submission of outgoing responses</li>
 * <li>Highest performing option.</li>
 * <li>Least deterministic behavior</li>
 * </ul>
 */
public class NonLockingAltContainer extends Container implements KeyspaceChangeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonLockingAltContainer.class);
    // This is a bad idea but only used to gate trace logging the invocation.

    private static final int SPIN_TRIES = 100;

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    private final StupidHashMap<Object, InstanceWrapper> instances = new StupidHashMap<>();

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    protected final AtomicInteger numBeingWorked = new AtomicInteger(0);

    public NonLockingAltContainer() {
        super(LOGGER);
    }

    protected NonLockingAltContainer(final Logger logger) {
        super(logger);
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
        if(maxPendingMessagesPerContainer >= 0)
            throw new IllegalStateException("Cannot use a " + NonLockingAltContainer.class.getPackage()
                + " container with the maxPendingMessagesPerContainer set for " + clusterId
                + " This container type does internal queuing. Please use the locking container.");

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

    protected static class WorkingQueueHolder {
        public final AtomicReference<LinkedList<KeyedMessage>> queue;

        public WorkingQueueHolder(final boolean locked) {
            queue = locked ? new AtomicReference<>(null) : new AtomicReference<>(new LinkedList<>());
        }
    }

    protected static <T> T setIfAbsent(final AtomicReference<T> ref, final Supplier<T> val) {
        do {
            final T maybeRet = ref.get();
            if(maybeRet != null)
                return maybeRet;

            final T value = val.get();
            if(ref.compareAndSet(null, value))
                return null;
        } while(true);
    }

    protected static class InstanceWrapper {
        public final Object instance;
        public boolean evicted = false;

        // the mailbox is free when the value is null. NOT the other way around.
        // If there is no current mailbox then nothing is working on this mp so
        // it's open to be worked.
        public final AtomicReference<WorkingQueueHolder> mailbox = new AtomicReference<>(null);

        public InstanceWrapper(final Object o) {
            this.instance = o;
        }

        // ----------------------------------------------------------------------------
        // Test access
        // ----------------------------------------------------------------------------
        protected Object getInstance() {
            return instance;
        }
    }

    protected final static class MutRef<X> {
        public X ref;

        public MutRef() {}

        public final X set(final X ref) {
            this.ref = ref;
            return ref;
        }
    }

    private <T> T waitFor(final Supplier<T> condition) {
        int counter = SPIN_TRIES;
        do {
            final T ret = condition.get();
            if(ret != null)
                return ret;
            if(counter > 0)
                counter--;
            else
                Thread.yield();
        } while(isRunning.get());
        throw new DempsyException("Not running.");
    }

    protected LinkedList<KeyedMessage> getQueue(final WorkingQueueHolder wp) {
        return waitFor(() -> wp.queue.getAndSet(null));
    }

    protected static <T> T pushPop(final LinkedList<T> q, final T toPush) {
        if(q.size() == 0)
            return toPush;
        q.add(toPush);
        return q.removeFirst();
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage keyedMessage, final boolean youOwnMessage) throws IllegalArgumentException, ContainerException {
        if(!isRunningLazy) {
            LOGGER.debug("Dispacth called on stopped container");
            statCollector.messageFailed(1);
        }

        if(keyedMessage == null)
            return; // No. We didn't process the null message

        if(keyedMessage.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

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

        numBeingWorked.incrementAndGet();

        boolean instanceDone = false;
        while(!instanceDone) {
            instanceDone = true;
            final InstanceWrapper wrapper = getInstanceForKey(messageKey);

            // wrapper will be null if the activate returns 'false'
            if(wrapper != null) {
                // final MutRef<WorkingQueueHolder> mref = new MutRef<>();
                boolean messageDone = false;
                while(!messageDone) {
                    messageDone = true;

                    final WorkingQueueHolder box = new WorkingQueueHolder(false);
                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> box);

                    // if mailbox is null then I got it.
                    if(mailbox == null) {
                        // final WorkingQueueHolder box = mref.ref; // can't be null if I got the mailbox
                        final LinkedList<KeyedMessage> q = getQueue(box); // spin until I get the queue
                        KeyedMessage toProcess = pushPop(q, new KeyedMessage(messageKey, actualMessage));
                        box.queue.lazySet(q); // put the queue back

                        while(toProcess != null) {
                            invokeOperationAndHandleDispose(wrapper.instance, Operation.handle, toProcess);
                            numBeingWorked.getAndDecrement();

                            // get the next message
                            final LinkedList<KeyedMessage> queue = getQueue(box);
                            if(queue.size() == 0)
                                // we need to leave the queue out or another thread can stuff
                                // a message into it and we'll loose the message when we reset
                                // the mailbox after breaking out of this loop
                                break;

                            toProcess = queue.removeFirst();
                            box.queue.lazySet(queue);
                        }

                        // release the mailbox
                        wrapper.mailbox.set(null);
                    } else {
                        // we didn't get exclusive access so let's see if we can add the message to the mailbox
                        // make one try at putting the message in the mailbox.
                        final LinkedList<KeyedMessage> q = mailbox.queue.getAndSet(null); // doesn't use getQueue because getQueue waits for the queue.

                        if(q != null) { // I got it!
                            q.add(new KeyedMessage(messageKey, actualMessage));
                            mailbox.queue.lazySet(q);
                        } else {
                            // see if we're evicted.
                            if(wrapper.evicted) {
                                instanceDone = false;
                                break; // start back at getting the instance.
                            }
                            messageDone = false; // start over from the top.
                        }
                    }
                }
            } else {
                // if we got here then the activate on the Mp explicitly returned 'false'
                if(LOGGER.isDebugEnabled())
                    LOGGER.debug("the container for " + clusterId + " failed to activate the Mp for " + SafeString.valueOf(prototype));
                disposition.dispose(actualMessage);
                // we consider this "processed"
                break; // leave the do/while loop
            }
        }

    }

    @Override
    protected void doevict(final EvictCheck check) {
        if(!check.isGenerallyEvitable() || !isRunning.get())
            return;

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();
        try (final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted();) {
            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Set<Object> keys = new HashSet<>(instances.size() + 10);
            keys.addAll(instances.keySet());

            while(keys.size() > 0 && instances.size() > 0 && isRunning.get() && !check.shouldStopEvicting()) {

                // store off anything that passes for later removal. This is to avoid a
                // ConcurrentModificationException.
                final Set<Object> keysProcessed = new HashSet<>();

                for(final Object key: keys) {
                    final InstanceWrapper wrapper = instances.get(key);

                    if(wrapper != null) { // if the MP still exists
                        final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(true)));
                        if(mailbox == null) { // this means I got it.

                            keysProcessed.add(key); // mark this to remove it from the set of keys later since we've
                                                    // dealing with it

                            // it was created locked so no one else will be able to drop messages in the mailbox.
                            final Object instance = wrapper.instance;
                            boolean evictMe;
                            try {
                                evictMe = check.shouldEvict(key, instance);
                            } catch(final RuntimeException e) {
                                LOGGER.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) +
                                    " resulted in an exception.", e.getCause());
                                evictMe = false;
                            }

                            if(evictMe) {
                                try {
                                    prototype.passivate(instance);
                                } catch(final Throwable e) {
                                    LOGGER.warn("Checking the eviction status/passivating of the Mp "
                                        + SafeString.objectDescription(instance) + " resulted in an exception.", e);
                                }

                                // even if passivate throws an exception, if the eviction check returned 'true' then
                                // we need to remove the instance.
                                instances.remove(key);
                                wrapper.evicted = true;
                                statCollector.messageProcessorDeleted(key);
                            } else {
                                wrapper.mailbox.set(null); // release the mailbox
                            }
                        } // end - I got the lock. Otherwise it's too busy to evict.
                    } // end if mp exists. Otherwise the mp is already gone.
                }
                keys.removeAll(keysProcessed); // remove the keys we already checked
            }
        }
    }

    // TODO: Output concurrency blocks normal message handling. Need a means of managing this better.
    // This method MUST NOT THROW
    @Override
    protected void outputPass() {
        if(!prototype.isOutputSupported())
            return;

        // take a snapshot of the current container state.
        final LinkedList<Object> toOutput = new LinkedList<>(instances.keySet());

        final Executor executorService = getOutputExecutorService();
        final Semaphore taskLock = (executorService != null) ? new Semaphore(outputConcurrency) : null;

        // This keeps track of the number of concurrently running
        // output tasks so that this method can wait until they're
        // all done to return.
        //
        // It's also used as a condition variable signaling on its
        // own state changes.
        final AtomicLong numExecutingOutputs = new AtomicLong(0);

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();

        // keep going until all of the outputs have been invoked
        while(toOutput.size() > 0 && isRunning.get()) {
            for(final Iterator<Object> iter = toOutput.iterator(); iter.hasNext();) {
                final Object key = iter.next();

                final InstanceWrapper wrapper = instances.get(key);

                if(wrapper != null) { // if the MP still exists
                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(true)));
                    if(mailbox == null) { // this means I got it.
                        // it was created locked so no one else will be able to drop messages in the mailbox.
                        final Semaphore taskSepaphore = taskLock;

                        // This task will release the wrapper's lock.
                        final Runnable task = new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    if(isRunning.get())
                                        invokeOperationAndHandleDispose(wrapper.instance, Operation.output, null);
                                } finally {
                                    wrapper.mailbox.set(null); // releases this back to the world

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
                                wrapper.mailbox.set(null); // we never got into the run so we need to release the lock
                                // this may happen because of a race condition between the
                                taskSepaphore.release();
                            } catch(final InterruptedException e) {
                                // this can happen while blocked in the semaphore.acquire.
                                // if we're no longer running we should just get out
                                // of here.
                                //
                                // Not releasing the taskSepaphore assumes the acquire never executed.
                                // if (since) the acquire never executed we also need to release the
                                // wrapper lock or that Mp will never be usable again.
                                wrapper.mailbox.set(null); // we never got into the run so we need to release the lock
                            }
                        } else
                            task.run();

                        iter.remove();

                    } // didn't get the lock
                } else { // end if mp exists. Otherwise the mp is already gone.
                    iter.remove();
                    LOGGER.warn("There was an attempt to output a non-existent Mp for key " + SafeString.objectDescription(key));
                }
            } // loop over every mp
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

    StupidHashMap<Object, Boolean> keysBeingWorked = new StupidHashMap<>();

    /**
     * This is required to return non null or throw a ContainerException
     */
    protected InstanceWrapper getInstanceForKey(final Object key) throws ContainerException {
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

            if(instance == null)
                throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                    SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                    ". The value returned from the clone call appears to be null.");

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
                    instance = null;
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
                instances.putIfAbsent(key, wrapper); // once it goes into the map, we can remove it from the 'being
                                                     // worked' set
                keysBeingWorked.remove(key); // remove it from the keysBeingWorked since any subsequent call will get
                // the newly added one.
                statCollector.messageProcessorCreated(key);
            }
            return wrapper;
        }
    }

}
