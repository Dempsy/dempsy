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

package net.dempsy.monitoring.basic;

import java.util.concurrent.atomic.AtomicLong;

import net.dempsy.container.NodeMetricGetters;
import net.dempsy.monitoring.NodeStatsCollector;

/**
 * A very basic implementation of StatsCollector.
 * Doesn't do all the fancy stuff the default Coda 
 * implementation does, but useful for testing.
 *
 */
public class BasicNodeStatsCollector implements NodeStatsCollector, NodeMetricGetters {
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesDiscarded = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesUnsent = new AtomicLong();

    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();

    private Gauge currentMessagesPendingGauge = null;
    private Gauge currentMessagesOutPendingGauge = null;

    @Override
    public long getDiscardedMessageCount() {
        return messagesDiscarded.longValue();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messagesDiscarded.incrementAndGet();
    }

    @Override
    public void messageNotSent() {
        messagesUnsent.incrementAndGet();
    }

    @Override
    public void messageReceived(final Object message) {
        messagesReceived.incrementAndGet();
    }

    @Override
    public void messageSent(final Object message) {
        messagesSent.incrementAndGet();
    }

    @Override
    public void stop() {
        // no-op

    }

    @Override
    public long getMessagesNotSentCount() {
        return messagesUnsent.get();
    }

    @Override
    public long getMessagesSentCount() {
        return messagesSent.get();
    }

    @Override
    public long getMessagesReceivedCount() {
        return messagesReceived.get();
    }

    @Override
    public long getMessageBytesSent() {
        return bytesSent.get();
    }

    @Override
    public long getMessageBytesReceived() {
        return bytesReceived.get();
    }

    @Override
    public synchronized void setMessagesPendingGauge(final Gauge currentMessagesPendingGauge) {
        this.currentMessagesPendingGauge = currentMessagesPendingGauge;
    }

    @Override
    public synchronized void setMessagesOutPendingGauge(final Gauge currentMessagesOutPendingGauge) {
        this.currentMessagesOutPendingGauge = currentMessagesOutPendingGauge;
    }

    @Override
    public synchronized long getMessagesPending() {
        return currentMessagesPendingGauge == null ? 0 : currentMessagesPendingGauge.value();
    }

    @Override
    public synchronized long getMessagesOutPending() {
        return currentMessagesOutPendingGauge == null ? 0 : currentMessagesOutPendingGauge.value();
    }

}
