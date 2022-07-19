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

import static net.dempsy.util.Functional.ignore;
import static net.dempsy.util.SafeString.objectDescription;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.threading.ThreadingModel;
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
public class NonLockingAltContainer extends Container {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonLockingAltContainer.class);
    // This is a bad idea but only used to gate trace logging the invocation.

    private static final int SPIN_TRIES = 100;

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    // private final ConcurrentHashMap<Object, InstanceWrapper> instances = new ConcurrentHashMap<>();
    private final StupidHashMap<Object, InstanceWrapper> instances = new StupidHashMap<>();

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    protected final AtomicInteger numBeingWorked = new AtomicInteger(0);

    protected ThreadingModel dempsyThreadingModel = null;

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

        dempsyThreadingModel = infra.getThreadingModel();

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
        public final AtomicReference<LinkedList<KeyedMessageWithOp>> queue;

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
        } while (true);
        // we CANNOT throw an exception from here, nor can we return null.
//        while(isRunning.get());
//        throw new DempsyException("Not running.");
    }

    protected LinkedList<KeyedMessageWithOp> getQueue(final WorkingQueueHolder wp) {
        return waitFor(() -> wp.queue.getAndSet(null));
    }

    protected static <T> T pushPop(final LinkedList<T> q, final T toPush) {
        if(q.size() == 0)
            return toPush;
        q.add(toPush);
        return q.removeFirst();
    }

    protected static class KeyedMessageWithOp extends KeyedMessage {
        public final Operation op;

        public KeyedMessageWithOp(final Object key, final Object message, final Operation op) {
            super(key, message);
            this.op = op;
        }

    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage keyedMessage, final Operation op, final boolean youOwnMessage) throws IllegalArgumentException, ContainerException {
        if(keyedMessage == null)
            return; // No. We didn't process the null message

        if(keyedMessage.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

        // we only use message disposition if we don't own the message (if the message just arrived,
        // then we do own the message) the AND we're actually handling a message (as opposed to
        // running an output cycle or an eviction).
        final boolean callDisposition = !youOwnMessage && op.handlesMessage;

        final Object actualMessage = callDisposition ? disposition.replicate(keyedMessage.message) : keyedMessage.message;
        final Object messageKey = keyedMessage.key;

        if(messageKey == null) {
            if(callDisposition)
                disposition.dispose(actualMessage);
            throw new ContainerException("Message " + objectDescription(actualMessage) + " contains no key.");
        }

        if(!inbound.doesMessageKeyBelongToNode(messageKey)) {
            if(callDisposition)
                disposition.dispose(actualMessage);
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("Message with key " + SafeString.objectDescription(messageKey) + " sent to wrong container. ");

            if(Operation.output != op)
                statCollector.messageFailed(1);
            return;
        }

        numBeingWorked.incrementAndGet();

        boolean instanceDone = false;
        while(!instanceDone) {
            instanceDone = true;
            final InstanceWrapper wrapper = getInstanceForKey(messageKey, actualMessage);

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
                        final LinkedList<KeyedMessageWithOp> q = getQueue(box); // spin until I get the queue

                        // if this is an output calculation then assume it's in the front of the queue
                        KeyedMessageWithOp toProcess = op == Operation.output ? new KeyedMessageWithOp(messageKey, actualMessage, op)
                            : pushPop(q, new KeyedMessageWithOp(messageKey, actualMessage, op));

                        box.queue.lazySet(q); // put the queue back

                        while(toProcess != null) {
                            invokeOperationAndHandleDispose(wrapper.instance, toProcess.op, toProcess);
                            numBeingWorked.getAndDecrement();

                            // get the next message
                            final LinkedList<KeyedMessageWithOp> queue = getQueue(box);
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
                        final LinkedList<KeyedMessageWithOp> q = mailbox.queue.getAndSet(null); // doesn't use getQueue because getQueue waits for the queue.

                        if(q != null) { // I got it!
                            q.add(new KeyedMessageWithOp(messageKey, actualMessage, op));
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
                if(callDisposition)
                    disposition.dispose(actualMessage);
                // we consider this "processed"
                break; // leave the do/while loop
            }
        }
    }

    @Override
    public boolean containerInternallyQueuesMessages() {
        return true;
    }

    @Override
    public boolean containerSupportsBulkProcessing() {
        return false;
    }

    @Override
    public void stop() {
        super.stop();

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();

        while(instances.size() > 0) {
            final Set<Object> keys = new HashSet<>(instances.size() + 10);
            keys.addAll(instances.keySet());

            for(final Object key: keys) {
                final InstanceWrapper wrapper = instances.get(key);

                if(wrapper != null) { // if the MP still exists
                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(true)));
                    if(mailbox == null) { // this means I got it.

                        // it was created locked so no one else will be able to drop messages in the mailbox.
                        final Object instance = wrapper.instance;
                        try {
                            prototype.passivate(instance);
                        } catch(final RuntimeException e) {
                            LOGGER.warn("Passivating of the Mp " + SafeString.objectDescription(instance) +
                                " resulted in an exception.", e.getCause());
                        }

                        instances.remove(key);
                        if(LOGGER.isDebugEnabled())
                            LOGGER.debug("[{}]: Passivating Mp for {}. {} remaining", clusterId, key, instances.size());
                        wrapper.evicted = true;
                        statCollector.messageProcessorDeleted(key);
                    }
                }
            }

        }
    }

    @Override
    protected void doevict(final EvictCheck check) {
        if(!check.isGenerallyEvitable() || !isRunning.get())
            return;

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();
        try(final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted();) {
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
                                    // even if passivate throws an exception, if the eviction check returned 'true' then
                                    // we need to remove the instance.
                                    LOGGER.warn("Checking the eviction status/passivating of the Mp "
                                        + SafeString.objectDescription(instance) + " resulted in an exception.", e);
                                }

                                instances.remove(key);
                                if(LOGGER.isDebugEnabled())
                                    LOGGER.debug("[{}]: Evicting/Actually removing Mp for {}. {} remaining", clusterId, key, instances.size());
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

    // This method MUST NOT THROW
    @Override
    protected void outputPass() {
        if(!prototype.isOutputSupported())
            return;

        // take a snapshot of the current container state.
        final ArrayList<Object> toOutput = new ArrayList<>(instances.keySet());
        if(toOutput.size() == 0)
            return;

        if(LOGGER.isDebugEnabled())
            LOGGER.debug("Output pass for {} on {} MPs", clusterId, toOutput.size());

        final AtomicLong numOutputJobs = new AtomicLong(0);
        for(final Object key: toOutput) {
            final InstanceWrapper wrapper = instances.get(key);
            // non-null means it still exists
            if(wrapper != null) {
                dempsyThreadingModel.submitPrioity(new OutputMessageJob(this, key, numOutputJobs));
                numOutputJobs.incrementAndGet();
            }
        }

        while(numOutputJobs.get() > 0 && isRunning.get())
            ignore(() -> Thread.sleep(1));
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    ConcurrentHashMap<Object, Boolean> keysBeingWorked = new ConcurrentHashMap<>();

    /**
     * This is required to return non null or throw a ContainerException
     */
    protected InstanceWrapper getInstanceForKey(final Object key, final Object actualMessage) throws ContainerException {
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
                        + " threw an exception when trying to create a new message processor for they key " + SafeString.objectDescription(key), e.userCause);
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
                    prototype.activate(instance, key, actualMessage);
                    activateSuccessful = true;
                }
            } catch(final DempsyException e) {
                if(e.userCaused()) {
                    LOGGER.warn("The message processor " + SafeString.objectDescription(instance) + " activate call threw an exception.", e.userCause);
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
