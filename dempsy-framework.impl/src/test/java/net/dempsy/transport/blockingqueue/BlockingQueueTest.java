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

package net.dempsy.transport.blockingqueue;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import net.dempsy.Infrastructure;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.TransportManager;
import net.dempsy.util.TestInfrastructure;

public class BlockingQueueTest {

    private static final String transportTypeId = BlockingQueueReceiver.class.getPackage().getName();

    /*
     * Test basic functionality for the BlockingQueue implementation of Message Transport. Verify that messages sent to the Sender arrive at the receiver via handleMessage.
     */
    @Test
    public void testBlockingQueue() throws Exception {
        final AtomicReference<String> message = new AtomicReference<String>(null);
        final ArrayBlockingQueue<Object> input = new ArrayBlockingQueue<>(16);
        try (final Receiver r = new BlockingQueueReceiver(input);
                final Infrastructure infra = new TestInfrastructure(new DefaultThreadingModel("BQTest-testBlockingQueue-"));
                final TransportManager tranMan = chain(new TransportManager(), c -> c.start(infra));
                SenderFactory sf = tranMan.getAssociatedInstance(transportTypeId);) {
            final Sender sender = sf.getSender(r.getAddress());
            r.start((final String msg) -> {
                message.set(new String(msg));
                return true;
            }, infra);
            sender.send("Hello");
            assertTrue(poll(o -> "Hello".equals(message.get())));
        }
    }

    /**
     * Test overflow for a blocking Transport around a queue with depth one. While the transport will not call the, and does not even have a , overflow handler, every message will call the overflow handler on
     * the receiver since the queue is always full.
     * 
     * @throws Throwable
     */
    @Test
    public void testBlockingQueueOverflow() throws Throwable {
        final AtomicReference<String> message = new AtomicReference<String>(null);
        final ArrayBlockingQueue<Object> input = new ArrayBlockingQueue<>(1);
        try (final Infrastructure infra = new TestInfrastructure(new DefaultThreadingModel("BQTest-testBlockingQueueOverflow-"));
                final Receiver r = new BlockingQueueReceiver(input);
                final TransportManager tranMan = chain(new TransportManager(), c -> c.start(infra));
                final SenderFactory sf = tranMan.getAssociatedInstance(transportTypeId);) {
            final Sender sender = sf.getSender(r.getAddress());

            final AtomicBoolean finallySent = new AtomicBoolean(false);
            final AtomicLong receiveCount = new AtomicLong();

            sender.send("Hello"); // fill up queue

            final Thread t = new Thread(() -> {
                try {
                    sender.send("Hello again");
                } catch (final MessageTransportException e) {
                    throw new RuntimeException(e);
                }
                finallySent.set(true);
            });
            t.start();

            Thread.sleep(100);
            assertFalse(finallySent.get()); // the thread should be hung blocked on the send

            // Start the receiver to read
            r.start((final String msg) -> {
                message.set(new String(msg));
                receiveCount.incrementAndGet();
                return true;
            }, infra);

            // 2 messages should have been read and the 2nd should be "Hello again"
            assertTrue(poll(o -> "Hello again".equals(message.get())));

            // The thread should shut down eventually
            assertTrue(poll(o -> !t.isAlive()));
        }
    }
}
