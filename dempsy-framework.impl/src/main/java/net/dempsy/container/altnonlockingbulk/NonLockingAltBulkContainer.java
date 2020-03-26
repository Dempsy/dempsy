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

package net.dempsy.container.altnonlockingbulk;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.container.ContainerException;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.util.SafeString;

/**
 * <p>
 * The {@link NonLockingAltBulkContainer} manages the lifecycle of message processors for the node that it's
 * instantiated in.
 * </p>
 *
 * <p>
 * Behavior:
 * </p>
 * <ul>
 * <li>Can't set maxPendingMessagesPerContainer</li>
 * <li>Internally queues messages (unlimited queue!)</li>
 * <li>handles bulk processing (therefore, can defer shedding to Mp)</li>
 * <li>Guarantee's order in submission of outgoing responses</li>
 * <li>Highest performing option.</li>
 * <li>Semi-deterministic behavior (because of Bulk processing)</li>
 * </ul>
 */
public class NonLockingAltBulkContainer extends NonLockingAltContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonLockingAltBulkContainer.class);

    public NonLockingAltBulkContainer() {
        super(LOGGER);
    }

    @Override
    public boolean containerInternallyQueuesMessages() {
        return true;
    }

    @Override
    public boolean containerSupportsBulkProcessing() {
        return true;
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage keyedMessage, final boolean youOwnMessage) throws IllegalArgumentException, ContainerException {
        if(!isRunningLazy) {
            LOGGER.debug("Dispacth called on stopped container");
            statCollector.messageFailed(1);
            if(youOwnMessage)
                disposition.dispose(keyedMessage.message);
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
                final MutRef<WorkingQueueHolder> mref = new MutRef<>();
                boolean messageDone = false;
                while(!messageDone) {
                    messageDone = true;

                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(false)));

                    // if mailbox is null then I got it.
                    if(mailbox == null) {
                        final WorkingQueueHolder box = mref.ref; // can't be null if I got the mailbox
                        boolean theresStillMessages = false;
                        boolean alreadyProcessedDispatched = false;
                        do {
                            {
                                final LinkedList<KeyedMessage> q = getQueue(box); // spin until I get the queue
                                final int queueLen = q.size();
                                if(queueLen == 0) { // then we're not doing bulk
                                    if(alreadyProcessedDispatched) {
                                        // DO NOT put the queue back since we're done.
                                        // Free the mailbox.
                                        wrapper.mailbox.set(null); // we're all set to leave.
                                        break; // from the do loop.
                                    }
                                    box.queue.lazySet(q); // put the queue back so new messages can begin building
                                    invokeOperationAndHandleDispose(wrapper.instance, Operation.handle, new KeyedMessage(messageKey, actualMessage));
                                    numBeingWorked.getAndDecrement();
                                } else if(queueLen == 1 && alreadyProcessedDispatched) {
                                    // in this case we have 1 message to dispatch and it's on the queue.
                                    final KeyedMessage toProcess = q.removeFirst();
                                    // put the empty queue back so new messages can begin building
                                    box.queue.lazySet(q);
                                    invokeOperationAndHandleDispose(wrapper.instance, Operation.handle, toProcess);
                                    numBeingWorked.getAndDecrement();
                                } else { // bulk - we'll have at least 2 messages
                                    // =================================================
                                    // copy the queue into toProcess so we can return the queue.
                                    // =================================================
                                    // Set toProcess using the fastest means we can.
                                    final KeyedMessage[] toProcessArr;
                                    if(alreadyProcessedDispatched) { // don't add the currently dispatched to the list
                                                                     // since we already did it
                                        toProcessArr = new KeyedMessage[queueLen];
                                        for(int qi = 0; qi < queueLen; qi++)
                                            toProcessArr[qi] = q.removeFirst();
                                    } else {
                                        toProcessArr = new KeyedMessage[queueLen + 1];
                                        for(int qi = 0; qi < queueLen; qi++)
                                            toProcessArr[qi] = q.removeFirst();
                                        toProcessArr[queueLen] = new KeyedMessage(messageKey, actualMessage);
                                    }

                                    // put the empty queue back so new messages can begin building
                                    box.queue.lazySet(q);

                                    final List<KeyedMessage> toProcess = Arrays.asList(toProcessArr);
                                    // =================================================

                                    invokeBulkHandleAndHandleDispose(wrapper.instance, toProcess);
                                    numBeingWorked.addAndGet(-toProcess.size());
                                }
                                alreadyProcessedDispatched = true;
                            }
                            // release the mailbox IFF the queue is still empty. If the queue
                            // managed to accumulate a couple additional messages then we'll
                            // loose them if we clear the mailbox
                            {
                                final LinkedList<KeyedMessage> q = getQueue(box);
                                if(q.size() == 0) {
                                    // We must leave the queue out in case another thread is interrogating
                                    // the mailbox in order to drop more messages into it. If the queue is
                                    // out then they can't until we free the entire mailbox.
                                    wrapper.mailbox.set(null); // we're all set to leave.
                                    theresStillMessages = false;
                                } else {
                                    box.queue.lazySet(q);
                                    theresStillMessages = true;
                                }
                            }
                        } while(theresStillMessages);
                    } else {
                        // we didn't get exclusive access so let's see if we can add the message to the mailbox
                        // make one try at putting the message in the mailbox.
                        final LinkedList<KeyedMessage> q = mailbox.queue.getAndSet(null);

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
}
