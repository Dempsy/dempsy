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
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener;
import com.nokia.dempsy.util.Pair;

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
       * that the message should go to. The {@link Pair} can optionally contain a second value which 
       * includes metadata for the message information. If non-null this metadata will be 
       * serialized along with the message itself and provided to the Inbound.
       * @throws DempsyException when something distasteful happens.
       */
      public Pair<Destination,Object> selectDestinationForMessage(Object messageKey, Object message) throws DempsyException;
      
      /**
       * The {@link Outbound} is responsible for providing the {@link ClusterId} for which it is the 
       * {@link Outbound} for.
       */
      public ClusterId getClusterId();
      
      /**
       * Since Outbound initialization can take place asynchronously, this method should be implemented
       * to return true when the outbound's first successful initialization is complete. It's used to
       * in tests to make sure that an application is completely initialized before proceeding.
       */
      public boolean completeInitialization();
      
      /**
       * @return all of the currently known destinations. This is called in order to manage
       * the clean shutdown of the transport when the cluster proxied by the {@link Outbound}
       * changes state.
       */
      public Collection<Destination> getKnownDestinations();
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
       * {@link MessageProcessor} are valid in 'this' node, it should be able to provide that
       * information through the implementation of this method. This is used as part of the
       * Pre-instantiation phase of the Message Processor's lifecycle.
       */
      public boolean doesMessageKeyBelongToNode(Object messageKey);
      
      /**
       * Shut down and reclaim any resources associated with the {@link Inbound} instance.
       */
      public void stop();
      
      /**
       * This is only called from tests
       * @return
       */
      public boolean isInitialized();
      
      /**
       * Since the responsibility for the portion of the keyspace that this node is responsible for
       * is determined by the Inbound strategy, when that responsibility changes, Dempsy itself
       * needs to be notified.
       */
      public static interface KeyspaceResponsibilityChangeListener
      {
         public void keyspaceResponsibilityChanged(boolean less, boolean more);
         
         public void setInboundStrategy(RoutingStrategy.Inbound inbound);
      }
   }
   
   /**
    * <p>This method will be called from the Dempsy framework in order to instantiate the one Inbound for 
    * 'this' node. Keep in mind that when running in LocalVm mode there can be more than one inbound per
    * process.</p>
    * 
    * <p>NOTE: The implementation should invoke the setInboundStrategy on the {@link KeyspaceResponsibilityChangeListener}
    * before returning the Inbound from this method.</p>
    *
    * @param cluster is the cluster information manager handle for 'this' node.
    * @param messageTypes is the types of messages that Dempsy determined could be handled by the {@link MessageProcessor}
    * in this node. This information should be shared across to the {@link Outbound} and registered with 
    * the {@link Coordinator} to allow Dempsy to restrict messages to the appropriate types.
    * @param thisDestination is the transport Destination instance that represents how to communicate
    * with 'this' node.
    * @return the {@link Inbound} instance.
    */
   public Inbound createInbound(ClusterInfoSession cluster, ClusterId clusterId, Collection<Class<?>> messageTypes,
                                Destination thisDestination, Inbound.KeyspaceResponsibilityChangeListener listener);
   
   /**
    * This class is responsible for creating Outbounds dynamically. In most implementations it will
    * register with the ClusterInfoSession for changes in the application topology 
    * (new clusters coming and going). The only interaction with this class is for the Router
    * to call stop on shutdown.
    */
   public static interface OutboundManager
   {
      /**
       * This will be called from Dempsy to notify the {@link OutboundManager} that it's
       * finished and can be shut down.
       */
      public void stop();
      
      /**
       * The RoutingStrategy is responsible for understanding which downstream cluster can 
       * handle which object instances by their type. Dempsy will deferr to the routing
       * strategy using this method. The likelyhood is that the {@link Outbound}s returned
       * will be used to route messages of that type to.
       */
      public Collection<Outbound> retrieveOutbounds(Class<?> messageType);
      
      /**
       * This interface will be implemented by the Dempsy framework. The OutboundManager
       * implementation needs to notify it when it detects that a cluster has been shut down.
       */
      public static interface ClusterStateMonitor
      {
         /**
          * It is possible to call this with an empty collection. This will simply reset the internal 
          * Dempsy state and force it to rediscover the available clusters. This should really
          * only be the fallback (for example, when the ClusterInfoSession is throwing exceptions and
          * so the current cluster information is unknown and should probably be rebuilt from scratch
          * anyway).
          */
         public void clusterStateChanged(Outbound oldOutboundThatChanged);
      }
      
      /**
       * The Dempsy framework will register for future state changes with the RoutingStrategy implementation
       * using this method. The RoutingStrategy implementation should call back on the instance provided
       * whenever the state of the cluster changes.
       */
      public void setClusterStateMonitor(ClusterStateMonitor monitor);
   }
   
   /**
    * The RoutingStrategy needs to create an {@link OutboundManager} needs to dynamcially react to changes in the 
    * Application topology. It should do this in a manner that absolutely succeeds even if the cluster information manager
    * is in a bad state. This is why this method takes stable parameters and throws no exception.
    *  
    * @param coordinator is the coordinator that the newly created {@link Outbound} can use to call back on the 
    * framework and supply (and remove) {@link Outbound}s
    * @param session is a handle to the session with the ClusterInfo manager. The {@link OutboundManager} should
    * register for changes to the application topology here and use the Cluster Info to create (and remove) the
    * appropriate {@link Outbound}s.
    * @param currentCluster is the ClusterId of the current cluster.
    * @return a new {@link OutboundManager} that manages the creation and registration (and unregistration) of
    * {@link Outbound}s.
    */
   public OutboundManager createOutboundManager(ClusterInfoSession session, ClusterId currentCluster,Collection<ClusterId> explicitClusterDestinations);
   
}

