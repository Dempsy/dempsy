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

package net.dempsy.transport.blockingqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;

/**
 * <p>
 * The Message transport default library comes with this BlockingQueue implementation.
 * </p>
 *
 * <p>
 * This class represents the Sender. You need to initialize it with the BlockingQueue to use.
 * </p>
 */
public class BlockingQueueSender implements Sender {

    private final BlockingQueue<Object> queue;
    private final NodeStatsCollector statsCollector;
    private final boolean blocking;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final BlockingQueueSenderFactory owner;

    /**
     * <p>
     * blocking is 'true' by default (after all this is a <b>Blocking</b>Queue implementation).
     * </p>
     *
     * <p>
     * If blocking is true then the transport will use the blocking calls on the BlockingQueue when sending, and therefore the OverflowHandler, whether set or
     * not, will never get called.</p?
     *
     * <p>
     * When blocking is 'false' a send to a full queue will result in:
     * <li>if the MessageOverflowHandler is set, it will pass the message to it.</li>
     * <li>otherwise it will throw a MessageTransportException.</li>
     * </p>
     *
     * @param blocking
     *     is whether or not to set this queue to blocking. It can be changed after a queue is started but there is no synchronization around the checking in
     *     the send method.
     */
    BlockingQueueSender(final BlockingQueueSenderFactory factory, final BlockingQueue<Object> queue, final boolean blocking,
        final NodeStatsCollector statsCollector) {
        this.statsCollector = statsCollector;
        this.queue = queue;
        this.blocking = blocking;
        this.owner = factory;
    }

    /**
     * Send a message into the BlockingQueueAdaptor.
     */
    @Override
    public void send(final Object message) throws MessageTransportException {
        if(shutdown.get())
            throw new MessageTransportException("send called on shutdown queue.");

        if(blocking) {
            while(true) {
                try {
                    queue.put(message);
                    if(statsCollector != null)
                        statsCollector.messageSent(message);
                    break;
                } catch(final InterruptedException ie) {
                    if(shutdown.get())
                        throw new MessageTransportException("Shutting down durring send.");
                }
            }
        } else {
            if(!queue.offer(message)) {
                if(statsCollector != null)
                    statsCollector.messageNotSent();
                throw new MessageTransportException("Failed to queue message due to capacity.");
            } else if(statsCollector != null)
                statsCollector.messageSent(message);
        }
    }

    @Override
    public void stop() {
        if(!shutdown.get()) {
            shutdown.set(true);
            owner.imDone(this);
        }
    }

    @Override
    public boolean considerMessageOwnsershipTransfered() {
        return true;
    }
}
