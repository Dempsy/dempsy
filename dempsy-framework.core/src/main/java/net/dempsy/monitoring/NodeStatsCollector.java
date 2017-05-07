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

package net.dempsy.monitoring;

import java.util.function.LongSupplier;

public interface NodeStatsCollector extends StatsCollector {

    /**
     * Sets a unique identifier for this node, used by certain stats collectors when publishing.
     */
    void setNodeId(final String nodeId);

    /**
     * The dispatcher calls this method in its <code>onMessage</code> handler.
     */
    void messageReceived(Object message);

    /**
     * The dispatcher calls this method in its <code>onMessage</code> handler when it discards a message.
     */
    void messageDiscarded(Object message);

    /**
     * Dispatcher calls this method when emitting a message
     */
    void messageSent(Object message);

    /**
     * Dispatcher calls this method when it fails to dispatch a message
     */
    void messageNotSent();

    /**
     * If the transport supports the queuing of incoming messages, then it can optionally supply a Gauge instance that provides this metric on demand.
     */
    void setMessagesPendingGauge(LongSupplier currentMessagesPendingGauge);

    /**
     * If the transport supports the queuing of outgoing messages, then it can optionally supply a Gauge instance that provides this metric on demand.
     */
    void setMessagesOutPendingGauge(LongSupplier currentMessagesOutPendingGauge);

}
