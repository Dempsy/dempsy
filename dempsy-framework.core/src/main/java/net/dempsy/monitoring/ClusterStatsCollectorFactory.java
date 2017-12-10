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

import net.dempsy.config.ClusterId;
import net.dempsy.transport.NodeAddress;

/**
 * <p>Core developers need to provide a factory for Dempsy to retrieve the chosen
 * {@link StatsCollector} implementation.</p>
 * 
 * <p>There are two provided with Dempsy.
 * The DropwizardClusterStatsCollector is built from Dropwizard Metrics and can be configured 
 * to connect to any number of delivery mechanisms include Ganglia and Graphite.</p>
 * 
 * <p>The BasicStatsCollector should only be used in tests as it doesn't provide
 * it's data anywhere but simply tracks the statistics.</p>
 * 
 */
public interface ClusterStatsCollectorFactory extends AutoCloseable {
    /**
    * The framework will invoke this method to retrieve the chosen {@link StatsCollector}
    * implementation. 
    * 
    * @param clusterId is the Cluster Id for this cluster. Should be used to categorize the
    * metrics being exposed.
    * @param nodeIdentification is the destination for this Node. If this node isn't a
    * destination for messages then this value can be null.
    * @return the particular {@link StatsCollector} implementation the coresponds to this
    * factory.
    */
    ClusterStatsCollector createStatsCollector(ClusterId clusterId, NodeAddress nodeIdentification);
}
