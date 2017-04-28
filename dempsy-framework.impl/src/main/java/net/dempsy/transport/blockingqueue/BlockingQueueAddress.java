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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.dempsy.transport.NodeAddress;

public class BlockingQueueAddress implements NodeAddress {
    private static final long serialVersionUID = 1L;

    protected static final Map<String, BlockingQueue<Object>> queues = new HashMap<>();

    protected final String guid;

    @SuppressWarnings("unused")
    private BlockingQueueAddress() {
        guid = null;
    }

    public BlockingQueueAddress(final BlockingQueue<Object> queue, final String guid) {
        this.guid = guid;

        synchronized (queues) {
            if (queues.containsKey(guid))
                throw new IllegalStateException("Queue " + guid + " already created.");
            queues.put(guid, queue);
        }
    }

    @Override
    public int hashCode() {
        return guid.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return (other == null || !(other instanceof BlockingQueueAddress)) ? false : guid.equals(((BlockingQueueAddress) other).guid);
    }

    @Override
    public String toString() {
        return guid;
    }

    @Override
    public String getGuid() {
        return guid;
    }

    public BlockingQueue<Object> getQueue() {
        synchronized (queues) {
            return queues.get(guid);
        }
    }

    public void close() {
        synchronized (queues) {
            queues.remove(guid);
        }
    }

    public static void completeReset() {
        synchronized (queues) {
            queues.values().forEach(bq -> bq.clear());
            queues.clear();
        }
    }
}
