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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.util.SafeString;

/**
 * <p>
 * The Message transport default library comes with this BlockingQueue implementation.
 * </p>
 *
 * <p>
 * This class represents both the MessageTransportSender and the concrete Adaptor (since BlockingQueues don't span process spaces). You need to initialize it
 * with the BlockingQueue to use, as well as the
 * MessageTransportListener to send messages to.
 * </p>
 *
 * <p>
 * Optionally you can provide it with a name that will be used in the thread that's started to read messages from the queue.
 * </p>
 */
public class BlockingQueueReceiver implements Runnable, Receiver {
    private static Logger LOGGER = LoggerFactory.getLogger(BlockingQueueReceiver.class);

    private final BlockingQueueAddress address;
    private final BlockingQueue<Object> queue;

    private Listener<Object> listener = null;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread currentThread = null;
    private boolean shutdown;

    private final static AtomicLong guidGenerator = new AtomicLong(0);

    public BlockingQueueReceiver(final BlockingQueue<Object> queue) {
        this.queue = queue;
        this.address = new BlockingQueueAddress(queue, "BlockingQueue_" + guidGenerator.getAndIncrement());
    }

    @Override
    public void run() {
        synchronized(this) {
            currentThread = Thread.currentThread();

            if(shutdown == true)
                return;

            running.set(true);
        }

        final Listener<Object> curListener = listener;

        // This check is cheap but unlocked
        while(!shutdown) {
            try {
                final Object val = queue.take();
                curListener.onMessage(val);
            } catch(final InterruptedException ie) {
                synchronized(this) {
                    // if we were interrupted we're probably stopping.
                    if(!shutdown)
                        LOGGER.warn("Superfluous interrupt.", ie);
                }
            } catch(final MessageTransportException err) {
                LOGGER.error("Exception while handling message.", err);
            }
        }

        running.set(false);
    }

    /**
     * A BlockingQueueAdaptor requires a MessageTransportListener to be set in order to adapt a client side.
     *
     * @param listener
     *     is the MessageTransportListener to push messages to when they come in.
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    @Override
    public synchronized void start(final Listener listener, final Infrastructure infra) {
        if(listener == null)
            throw new IllegalArgumentException("Cannot pass null to " + BlockingQueueReceiver.class.getSimpleName() + ".setListener");
        if(this.listener != null)
            throw new IllegalStateException(
                "Cannot set a new Listener (" + SafeString.objectDescription(listener) + ") on a " + BlockingQueueReceiver.class.getSimpleName()
                    + " when there's one already set (" + SafeString.objectDescription(this.listener) + ")");
        this.listener = listener;
        infra.getThreadingModel().runDaemon(this, "BQReceiver-" + address.toString());
    }

    @Override
    public void close() {
        synchronized(this) {
            shutdown = true;
        }

        while(running.get()) {
            if(currentThread != null)
                currentThread.interrupt();
            Thread.yield();
        }

        address.close();
    }

    @Override
    public NodeAddress getAddress(final Infrastructure infra) {
        return this.address;
    }

}
