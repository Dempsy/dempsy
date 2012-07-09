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

import java.util.Collection;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.router.RoutingStrategy.Outbound.Coordinator;

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
 * <p>As an example, the DecentralizedRoutingStrategy's Inbound and Outbound coordinate
 * through the cluster. The Inbound side negotiates for slot ownership and those
 * slots contain enough information for the Outbound side </p>
 * 
 * <p>Implementations must be able to handle multi-threaded access.</p>
 */
public interface RoutingStrategy
{
   /**
    * For every possible destination from a node to a downstream cluster there is an instance of an Outbound.
    * The responsibility of the Outbound is to reliably determine which node (Destination) to send each
    * message to for a given downstream cluster. 
    */
   public static interface Outbound
   {
      /**
       * This method needs to be implemented to determine the specific node that the outgoing
       * message is to be routed to.
       * 
       * @param messageKey is the message key for the message to be routed
       * @param message is the message to be routed.
       * @return a transport Destination indicating the unique node in the downstream cluster 
       * that the message should go to.
       * @throws DempsyException when something distasteful happens.
       */
      public Destination selectDestinationForMessage(Object messageKey, Object message) throws DempsyException;
      
      /**
       * The {@link Outbound} is responsible for providing the {@link ClusterId} for which it is the 
       * {@link Outbound} for.
       */
      public ClusterId getClusterId();
      
      /**
       * <p>Each node can have many Outbound instances and those Outbound cluster references
       * can come and go. In order to tell Dempsy about what's going on in the cluster
       * the Outbound should be updating the state of the OutboundCoordinator.</p>
       * 
       * <p>Implementors of the RoutingStrategy do not need to implement this interface.
       * There is only one implementation and that instance will be supplied by the
       * framework.</p>
       */
      public static interface Coordinator
      {
         /**
          * registers the outbound with the Coordinator and provide what types the destination
          * cluster can handle. Note that the Outbound is allowed to call registerOutbound 
          * more than once, without calling unregisterOutbound first but the results should
          * be the same.
          */
         public void registerOutbound(Outbound outbound, Collection<Class<?>> classes);
         
         /**
          * registers the outbound with the Coordinator and provide what types the destination
          * cluster can handle.
          */
         public void unregisterOutbound(Outbound outbound);

      }
      
      /**
       * Shut down and reclaim any resources associated with the {@link Outbound} instance.
       */
      public void stop();
      
   }
   
   /**
    * The primary responsibility of an Inbound is to set the appropriate information in the cluster
    * manager associated with the node within which the Inbound was instantiated. As described
    * for the {@link RoutingStrategy}, the Inbound should supply information to the cluster manager 
    * that the {@link Outbound} from an upstream node will use to determine which messages are
    * targeted to 'this' node.
    */
   public static interface Inbound
   {
      /**
       * Since the {@link Inbound} has the responsibility to determine which instances of a 
       * {@link MessageProcessor} are valid in 'this' node, it should be able to privide that
       * information through the implementataion of this method. This is used as part of the
       * Pre-instantiation phase of the Message Processor's lifecylce.
       */
      public boolean doesMessageKeyBelongToNode(Object messageKey);
      
      /**
       * Shut down and reclaim any resources associated with the {@link Inbound} instance.
       */
      public void stop();
   }
   
   /**
    * This method will be called from the Dempsy framework in order to instantiate the one Inbound for 
    * 'this' node. Keep in mind that when running in LocalVm mode there can be more than one inbound per
    * process.
    *
    * @param cluster is the cluster information manager handle for 'this' node.
    * @param messageTypes is the types of messages that Dempsy determined could be handled by the {@link MessageProcessor}
    * in this node. This information should be shared across to the {@link Outbound} and registered with 
    * the {@link Coordinator} to allow Dempsy to restrict messages to the appropriate types.
    * @param thisDestination is the transport Destination instance that represents how to communicate
    * with 'this' node.
    * @return the {@link Inbound} instance.
    */
   public Inbound createInbound(MpCluster<ClusterInformation, SlotInformation> cluster, Collection<Class<?>> messageTypes, Destination thisDestination);
   
   /**
    * The RoutingStrategy needs to create an {@link Outbound} that corresponds to the given cluster. It should do this
    * in a manner that absolutely succeeds even if the cluster information manager is in a bad state. This is why
    * this method takes stable parameters and throws no exception.
    *  
    * @param coordinator is the coordinator that the newly created {@link Outbound} can use to call back on the 
    * framework.
    * @param clusterId is the cluster id that the {@link Outbound} is being created for.
    * @return a new {@link Outbound} that manages the selection of a {@link Destination} given a message destined for 
    * the given cluster.
    */
   public Outbound createOutbound(Outbound.Coordinator coordinator, MpCluster<ClusterInformation, SlotInformation> cluster);
   
}

