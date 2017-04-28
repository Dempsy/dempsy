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

import java.util.HashMap;
import java.util.Map;

import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollectorFactory;
import net.dempsy.transport.NodeAddress;

public class BasicStatsCollectorFactory implements ClusterStatsCollectorFactory {
    Map<ClusterId, BasicClusterStatsCollector> collectors = new HashMap<>();

    @Override
    public synchronized BasicClusterStatsCollector createStatsCollector(final ClusterId clusterId, final NodeAddress listenerDestination) {
        BasicClusterStatsCollector ret = collectors.get(clusterId);
        if (ret == null) {
            ret = new BasicClusterStatsCollector();
            collectors.put(clusterId, ret);
        }
        return ret;
    }

    @Override
    public synchronized void close() throws Exception {
        for (final Map.Entry<ClusterId, BasicClusterStatsCollector> e : collectors.entrySet()) {
            e.getValue().close();
        }
        collectors.clear();
    }

}
