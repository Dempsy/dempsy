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

package com.nokia.dempsy.router;

import java.util.List;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;

/**
 * <p>A {@link RoutingStrategy} is responsible for determining how to find the appropriate
 * Node within a given cluster given a {@link MessageKey}. It has both an {@link Inbound}
 * side and an {@link Outbound} side that work together using the {@link MpCluster} to
 * do the bookkeeping necessary.</p>
 * 
 * <p>A simple example would be: a non-elastic, fixed-width cluster {@link RoutingStrategy} that
 * simply selects the Node to send a message to based on the <code>mod ('%' operator) of the hash
 * of a message's key with the number of nodes in the cluster.</p>
 * 
 * <p>In this example the {@link Inbound} strategy, which would be instantiated in each node,
 * would be implemented to register the its current Node with the {@link MpCluster}. The {@link Outbound}
 * side would use the registered information to select the appropriate Node.</p> 
 * 
 * <p>As mentioned, RoutingStrategy implementations need to be balanced. The {@link Inbound} can
 * safely assume that the {@link Outbound} created from the same strategy was responsible for 
 * setting up the cluster.</p>
 * 
 * <p>{@link Inbound} and {@link Outbound} need to be thought of in terms of the stages of a
 * pipeline in a Dempsy application. E.g.: 
 * <pre>
 * <code>
 * Adaptor --> inbound1|Stage1|outbound2 --> inbound2|Stage2|outbound3 ...
 * </code>
 * </pre>
 * 
 * <p>Notice that <code>outbound2</code> and <code>inbound2</code> require coordination
 * and are therefore defined in the {@link ApplicationDefinition} using a single {@link RoutingStrategy} as
 * part of the {@link ClusterDefinition} that defines <code>Stage2</code>
 * 
 * <p>As an example, the DefaultRoutingStrategy's Inbound and Outbound coordinate
 * through the cluster. The Inbound side negotiates for slot ownership and those
 * slots contain enough information for the Outbound side </p>
 * 
 * <p>Implementations must be able to handle multi-threaded access.</p>
 */
public interface RoutingStrategy
{
   public static interface Outbound
   {
      public SlotInformation selectSlotForMessageKey(Object messageKey) throws DempsyException;

      /**
       * resetCluster is called when the cluster for the Outbound side changes. In this
       * way implementations of this class do not need to be MpClusterWatchers
       * @param cluster - the cluster handle containing the new state.
       * @throws MpClusterException when the implementation has a problem accessing the cluster
       */
      public void resetCluster(MpCluster<ClusterInformation, SlotInformation> cluster) throws MpClusterException;
   }
   
   public static interface Inbound
   {
      /**
       * <p>resetCluster is called when the cluster for the Inbound side changes. In this
       * way implementations of this class do not need to be MpClusterWatchers.</p>
       * 
       * @param cluster - the cluster handle containing the new state.
       * @throws MpClusterException when the implementation has a problem accessing the cluster
       */
      public void resetCluster(MpCluster<ClusterInformation, SlotInformation> cluster,
            List<Class<?>> messageTypes, Destination thisDestination) throws MpClusterException;
      
      public boolean doesMessageKeyBelongToNode(Object messageKey);
   }
   
   public Inbound createInbound();
   
   public Outbound createOutbound();
   
}

