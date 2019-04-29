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

package net.dempsy.container.nonlocking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import net.dempsy.container.Container;
import net.dempsy.container.ContainerException;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.util.SafeString;
import net.dempsy.util.StupidHashMap;

/**
 * <p>
 * The {@link NonLockingContainer} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 *
 * The container is simple in that it does no thread management. When it's called it assumes that the transport
 * has provided the thread that's needed
 */
public class NonLockingContainer extends Container {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonLockingContainer.class);
    // This is a bad idea but only used to gate trace logging the invocation.
    private static final boolean traceEnabled = LOGGER.isTraceEnabled();

    private final StupidHashMap<Object, WorkingPlaceholder> working = new StupidHashMap<>();
    private final StupidHashMap<Object, Object> instances = new StupidHashMap<>();

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final AtomicInteger numBeingWorked = new AtomicInteger(0);

    private static class WorkingQueueHolder {
        LinkedList<KeyedMessage> queue = null;
    }

    protected static class WorkingPlaceholder {
        AtomicReference<WorkingQueueHolder> mailbox = new AtomicReference<>(new WorkingQueueHolder());
    }

    public NonLockingContainer() {
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
        return instances.get(key);
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    // this is called directly from tests but shouldn't be accessed otherwise.

    private Object createAndActivate(final Object key) throws ContainerException {
        Object instance = null;
        try {
            instance = prototype.newInstance();
        } catch(final DempsyException e) {
            if(e.userCaused()) {
                LOGGER.warn("The message processor prototype " + SafeString.valueOf(prototype)
                    + " threw an exception when trying to create a new message processor for they key " + SafeString.objectDescription(key));
                statCollector.messageFailed(true);
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
                LOGGER.warn("The message processor " + SafeString.objectDescription(instance) + " activate call threw an exception.");
                statCollector.messageFailed(true);
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
            if(instances.putIfAbsent(key, instance) != null) // once it goes into the map, we can remove it from the 'being worked' set
                throw new IllegalStateException("WTF?");
            // the newly added one.
            statCollector.messageProcessorCreated(key);
        }
        return instance;
    }

    private static final int SPIN_TRIES = 100;

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
        } while(true);
    }

    private WorkingQueueHolder getQueue(final WorkingPlaceholder wp) {
        return waitFor(() -> wp.mailbox.getAndSet(null));
    }

    private static final <T> T putIfAbsent(final StupidHashMap<Object, T> map, final Object key, final Supplier<T> value) {
        // final T ret = map.get(key);
        // if (ret == null)
        // return map.putIfAbsent(key, value);
        // return ret;
        return map.computeIfAbsent(key, value);
    }

    final static class MutRef<X> {
        public X ref;

        public final X set(final X ref) {
            this.ref = ref;
            return ref;
        }
    }

    @Override
    public void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException {
        if(!isRunningLazy) {
            LOGGER.debug("Dispacth called on stopped container");
            statCollector.messageFailed(false);
        }

        if(message == null)
            return; // No. We didn't process the null message

        if(message.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

        if(message.key == null)
            throw new ContainerException("Message " + objectDescription(message.message) + " contains no key.");

        if(!inbound.doesMessageKeyBelongToNode(message.key)) {
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("Message with key " + SafeString.objectDescription(message.key) + " sent to wrong container. ");
            statCollector.messageFailed(false);
            return;
        }

        final Object key = message.key;

        boolean keepTrying = true;
        while(keepTrying) {

            final MutRef<WorkingPlaceholder> wph = new MutRef<>();
            final WorkingPlaceholder alreadyThere = putIfAbsent(working, key, () -> wph.set(new WorkingPlaceholder()));

            if(alreadyThere == null) { // we're it!
                final WorkingPlaceholder wp = wph.ref;
                keepTrying = false; // we're not going to keep trying.
                List<KeyedMessageWithType> response = null; // these will be dispatched while NOT having the lock
                try { // if we don't get the WorkingPlaceholder out of the working map then that Mp will forever be lost.
                    numBeingWorked.incrementAndGet(); // we're working one.

                    Object instance = instances.get(key);
                    if(instance == null) {
                        try {
                            // this can throw
                            instance = createAndActivate(key);
                        } catch(final RuntimeException e) { // container or runtime exception
                            // This will drain the swamp
                            LOGGER.debug("Failed to process message with key " + SafeString.objectDescription(message.key), e);
                            instance = null;
                        }
                    }

                    if(instance == null) { // activation or creation failed.
                        numBeingWorked.decrementAndGet(); // decrement for this one
                        LOGGER.debug("Can't handle message {} because the creation of the Mp seems to have failed.",
                            SafeString.objectDescription(key));
                        final WorkingQueueHolder mailbox = getQueue(wp);
                        if(mailbox.queue != null) {
                            mailbox.queue.forEach(m -> {
                                LOGGER.debug("Failed to process message with key " + SafeString.objectDescription(m.key));
                                statCollector.messageFailed(true);
                                numBeingWorked.decrementAndGet(); // decrement for each in the queue
                            });
                        }
                    } else {
                        KeyedMessage curMessage = message;
                        while(curMessage != null) { // can't be null the first time
                            final List<KeyedMessageWithType> resp = invokeOperation(instance, Operation.handle, curMessage);
                            if(resp != null) { // these responses will be dispatched after we release the lock.
                                if(response == null)
                                    response = new ArrayList<>();
                                response.addAll(resp);
                            }

                            numBeingWorked.decrementAndGet(); // decrement the initial increment.

                            // work off the queue.
                            final WorkingQueueHolder mailbox = getQueue(wp); // spin until I have it.
                            if(mailbox.queue != null && mailbox.queue.size() > 0) { // if there are messages in the queue
                                curMessage = mailbox.queue.removeFirst(); // take a message off the queue
                                // curMessage CAN'T be NULL!!!!

                                // releasing the lock on the mailbox ... we're ready to process 'curMessage' on the next loop
                                wp.mailbox.set(mailbox);
                            } else {
                                curMessage = null;
                                // (1) NOTE: DON'T put the queue back. This will prevent ALL other threads trying to drop a message
                                // in this box. When an alternate thread tries to open the mailbox to put a message in, if it can't,
                                // because THIS thread's left it locked, the other thread starts the process from the beginning
                                // re-attempting to get exclusive control over the Mp. In other words, the other thread only makes
                                // a single attempt and if it fails it goes back to attempting to get the Mp from the beginning.
                                //
                                // This thread cannot give up the current Mp if there's a potential for any data to end up in the
                                // queue. Since we're about to give up the Mp we cannot allow the mailbox to become available
                                // therefore we cannot allow any other threads to spin on it.
                            }
                        }
                    }
                } finally {
                    if(working.remove(key) == null)
                        LOGGER.error("IMPOSSIBLE! Null key removed from working set.", new RuntimeException());
                }
                if(response != null) {
                    try {
                        dispatcher.dispatch(response);
                    } catch(final Exception de) {
                        LOGGER.warn("Failed on subsequent dispatch of " + response + ": " + de.getLocalizedMessage());
                    }
                }
            } else { // ... we didn't get the lock
                if(!block) { // blocking means no collisions allowed.
                    if(LOGGER.isTraceEnabled())
                        LOGGER.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
                    statCollector.messageCollision(message);
                    keepTrying = false;
                } else {
                    // try and get the queue.
                    final WorkingQueueHolder mailbox = alreadyThere.mailbox.getAndSet(null);

                    if(mailbox != null) { // we got the queue!
                        try {
                            keepTrying = false;
                            // drop a message in the mailbox queue and mark it as being worked.
                            numBeingWorked.incrementAndGet();
                            if(mailbox.queue == null)
                                mailbox.queue = new LinkedList<>();
                            mailbox.queue.add(message);
                        } finally {
                            // put it back - releasing the lock
                            alreadyThere.mailbox.set(mailbox);
                        }
                    } else { // if we didn't get the queue, we need to start completely over.
                             // otherwise there's a potential race condition - see the note at (1).
                        // we failed to get the queue ... maybe we'll have better luck next time.
                    }
                } // we didn't get the lock and we're blocking and we're now done handling the mailbox
            } // we didn't get the lock so we tried the mailbox (or ended becasuse we're non-blocking)
        } // keep working

    }

    @Override
    public void doevict(final EvictCheck check) {
        if(!check.isGenerallyEvitable() || !isRunning.get())
            return;

        try (final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted();) {

            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Set<Object> keys = new HashSet<>(instances.size() + 10);
            keys.addAll(instances.keySet());

            while(keys.size() > 0 && instances.size() > 0 && isRunning.get() && !check.shouldStopEvicting()) {

                // store off anything that passes for later removal. This is to avoid a
                // ConcurrentModificationException.
                final Set<Object> keysProcessed = new HashSet<Object>();

                for(final Object key: keys) {

                    final WorkingPlaceholder wp = new WorkingPlaceholder();
                    // we're going to hold up all incomming message to this mp
                    wp.mailbox.getAndSet(null); // this blocks other threads from
                                                // dropping messages in the mailbox

                    final WorkingPlaceholder alreadyThere = working.putIfAbsent(key, wp); // try to get a lock

                    if(alreadyThere == null) { // we got it the lock
                        try {
                            final Object instance = instances.get(key);

                            if(instance != null) {
                                keysProcessed.add(key); // track this key to remove it from the keys set later.

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
                                    statCollector.messageProcessorDeleted(key);
                                }
                            } else {
                                LOGGER.warn("There was an attempt to evict a non-existent Mp for key " + SafeString.objectDescription(key));
                            }
                        } finally {
                            working.remove(key); // releases this back to the world
                        }
                    }
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
        final LinkedList<Object> toOutput = new LinkedList<Object>(instances.keySet());

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
            for(final Iterator<Object> iter = toOutput.iterator(); iter.hasNext();) {
                final Object key = iter.next();

                final WorkingPlaceholder wp = new WorkingPlaceholder();
                // we're going to hold up all incomming message to this mp
                wp.mailbox.getAndSet(null); // this blocks other threads from
                                            // dropping messages in the mailbox

                final WorkingPlaceholder alreadyThere = working.putIfAbsent(key, wp); // try to get a lock
                if(alreadyThere == null) { // we got it the lock
                    final Object instance = instances.get(key);

                    if(instance != null) {
                        final Semaphore taskSepaphore = taskLock;

                        // This task will release the wrapper's lock.
                        final Runnable task = new Runnable() {

                            @Override
                            public void run() {
                                final List<KeyedMessageWithType> response;
                                try {
                                    if(isRunning.get())
                                        response = invokeOperation(instance, Operation.output, null);
                                    else
                                        response = null;
                                } finally {
                                    working.remove(key); // releases this back to the world

                                    // this signals that we're done.
                                    synchronized(numExecutingOutputs) {
                                        numExecutingOutputs.decrementAndGet();
                                        numExecutingOutputs.notifyAll();
                                    }
                                    if(taskSepaphore != null)
                                        taskSepaphore.release();
                                }

                                if(response != null) {
                                    try {
                                        dispatcher.dispatch(response);
                                    } catch(final Exception de) {
                                        if(isRunning.get())
                                            LOGGER.warn("Failed on subsequent dispatch of " + response + ": " + de.getLocalizedMessage());
                                    }
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
                                working.remove(key); // we never got into the run so we need to release the lock
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
                                working.remove(key); // we never got into the run so we need to release the lock
                            }
                        } else
                            task.run();

                        iter.remove();

                    } else {
                        working.remove(key);
                        LOGGER.warn("There was an attempt to evict a non-existent Mp for key " + SafeString.objectDescription(key));
                    }
                } // didn't get the lock
            } // loop over every mp
        } // end while there are still Mps that haven't had output invoked.

        // =======================================================
        // now make sure all of the running tasks have completed
        synchronized(numExecutingOutputs) {
            while(numExecutingOutputs.get() > 0) {
                try {
                    numExecutingOutputs.wait();
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

    @Override
    public void invokeOutput() {
        try (final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted();) {
            outputPass();
        }
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    public enum Operation {
        handle, output
    };

    /**
     * helper method to invoke an operation (handle a message or run output) handling all of the exceptions and forwarding any results.
     */
    private List<KeyedMessageWithType> invokeOperation(final Object instance, final Operation op, final KeyedMessage message) {
        if(instance != null) { // possibly passivated ...
            List<KeyedMessageWithType> result;
            try {
                if(traceEnabled)
                    LOGGER.trace("invoking \"{}\" for {}", SafeString.valueOf(instance), message);
                statCollector.messageDispatched(message);
                result = op == Operation.output ? prototype.invokeOutput(instance) : prototype.invoke(instance, message);
                statCollector.messageProcessed(message);
            } catch(final ContainerException e) {
                result = null;
                LOGGER.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " +
                    SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + objectDescription(message)) : ""), e);
                statCollector.messageFailed(false);
            }
            // this is an exception thrown as a result of the reflected call having an illegal argument.
            // This should actually be impossible since the container itself manages the calling.
            catch(final IllegalArgumentException e) {
                result = null;
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                    " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?",
                    e);
                statCollector.messageFailed(true);
            }
            // The app threw an exception.
            catch(final DempsyException e) {
                result = null;
                LOGGER.warn("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                    " because an exception was thrown by the Message Processeor itself.", e);
                statCollector.messageFailed(true);
            }
            // RuntimeExceptions bookeeping
            catch(final RuntimeException e) {
                result = null;
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                    " due to an unknown exception.", e);
                statCollector.messageFailed(false);

                if(op == Operation.handle)
                    throw e;
            }
            return result;
        }
        return null;
    }

}
