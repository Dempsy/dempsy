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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.container.internal.AnnotatedMethodInvoker;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.router.RoutingStrategy.Outbound;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

/**
 * <p>This class implements the routing for all messages leaving a node. Please note:
 * This object is meant to be instantiated and manipulated by the {@link Dempsy} 
 * orchestrator and not used directly. However, it is important to understand how routing within
 * Dempsy works.</p>
 * 
 * <p>Routing a message to a message processor happens in three stages. Given an 
 * {@link ApplicationDefinition} that contains many message processor clusters, messages 
 * leaving any one {@link Node} need to be routed to the appropriate message processors
 * in other clusters. The stages are as follows:</p>
 * 
 * <p><li>Using the message's type information (and {@link ClusterDefinition} if "destinations"
 * are set) determine the cluster that contains the message processor that the message
 * needs to be sent to.</li>
 *  
 * <li>Within the cluster determine the {@link Node} currently responsible for processing that
 * message using the messages key ({@link MessageKey}) and the current {@link RoutingStrategy}
 * for that cluster.</li>
 * 
 * <li>Once the message is sent to the appropriate {@link Node} the {@link MpContainer}
 * is responsible for routing the message to the appropriate {@link MessageProcessor}</li></p>
 * 
 * <p>As mentioned, if the particular cluster that the node that this Router is instantiated in 
 * has explicitly defined destinations, then the message routing will be limited to
 * only those destinations.</p>
 * 
 * <p>A router requires a non-null ApplicationDefinition during construction.</p>
 */
public class Router implements Dispatcher, RoutingStrategy.OutboundManager.ClusterStateMonitor
{
   private static Logger logger = LoggerFactory.getLogger(Router.class);

   private AnnotatedMethodInvoker methodInvoker = new AnnotatedMethodInvoker(MessageKey.class);
   private ClusterDefinition currentClusterDefinition = null;

   protected RoutingStrategy.OutboundManager outboundManager = null;
   
   private ClusterInfoSession mpClusterSession = null;
   private SenderFactory senderFactory;
   private StatsCollector statsCollector = null;
   private Serializer<Object> serializer;
   
   protected Set<Class<?>> stopTryingToSendTheseTypes = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());
   
   @SuppressWarnings("unchecked")
   public Router(ClusterDefinition currentClusterDefinition)
   {
      if (currentClusterDefinition == null)
         throw new IllegalArgumentException("Can't pass a null currentClusterDefinition to a " + SafeString.valueOfClass(this));
      this.currentClusterDefinition = currentClusterDefinition;
      this.serializer = (Serializer<Object>)currentClusterDefinition.getSerializer();
   }

   /**
    * Provide the handle to the cluster factory so that each visible cluster can be reached.
    */
   public void setClusterSession(ClusterInfoSession factory) { mpClusterSession = factory; }
   
   /**
    * @return a reference to the set ClusterInfoSession
    */
   public ClusterInfoSession getClusterSession() { return mpClusterSession; }
   
   /**
    * This sets the default {@link Transport} to use for each cluster a message may be routed to.
    * This can be overridden on a per-cluster basis.
    */
   public void setDefaultSenderFactory(SenderFactory senderFactory) { this.senderFactory = senderFactory; }
   
   /**
    * This sets the StatsCollector to log messages sent via this dispatcher to.
    */
   public void setStatsCollector(StatsCollector statsCollector) { this.statsCollector = statsCollector; }
   
   /**
    * Prior to the {@link Router} being used it needs to be initialized.
    */
   public void start() throws ClusterInfoException,DempsyException
   {
      // get the set of explicit destinations if they exist
      Set<ClusterId> explicitClusterDestinations = currentClusterDefinition.hasExplicitDestinations() ? new HashSet<ClusterId>() : null;
      if (explicitClusterDestinations != null)
         explicitClusterDestinations.addAll(Arrays.asList(currentClusterDefinition.getDestinations()));
      
      RoutingStrategy strategy = (RoutingStrategy)currentClusterDefinition.getRoutingStrategy();
      outboundManager = strategy.createOutboundManager(mpClusterSession, currentClusterDefinition.getClusterId(), explicitClusterDestinations);
      outboundManager.setClusterStateMonitor(this);
   }
   
   @Override
   public ClusterId getThisClusterId() { return currentCluster; }

   /**
    * A {@link Router} is also a {@link Dispatcher} that is the instance that's typically
    * injected into {@link Adaptor}s. The implementation of this dispatch routes the message
    * to the appropriate {@link MessageProcessor} in the appropriate {@link ClusterDefinition}
    */
   @Override
   public void dispatch(Object message)
   {
      if(message == null)
      {
         logger.warn("Attempt to dispatch null message.");
         return;
      }
      
      List<Object> messages = new ArrayList<Object>();
      getMessages(message, messages);
      for(Object msg: messages)
      {
         Class<?> messageClass = msg.getClass();
         
         Object msgKeysValue = null;
         try
         {
            if (!stopTryingToSendTheseTypes.contains(messageClass))
               msgKeysValue = methodInvoker.invokeGetter(msg);
         }
         catch(IllegalArgumentException e1)
         {
            stopTryingToSendTheseTypes.add(msg.getClass());
            logger.warn("unable to retrieve key from message: " + String.valueOf(message) + 
                  (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") + 
                  "\" Please make sure its has a simple getter appropriately annotated: " + 
                  e1.getLocalizedMessage()); // no stack trace.
         }
         catch(IllegalAccessException e1)
         {
            stopTryingToSendTheseTypes.add(msg.getClass());
            logger.warn("unable to retrieve key from message: " + String.valueOf(message) + 
                  (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") + 
                  "\" Please make sure all annotated getter access is public: " + 
                  e1.getLocalizedMessage()); // no stack trace.
         }
         catch(InvocationTargetException e1)
         {
            logger.warn("unable to retrieve key from message: " + String.valueOf(message) + 
                  (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") + 
                  "\" due to an exception thrown from the getter: " + 
                  e1.getLocalizedMessage(),e1.getCause());
         }
         
         if(msgKeysValue != null)
         {
            Collection<RoutingStrategy.Outbound> routers = outboundManager.retrieveOutbounds(msg.getClass());
            if(routers != null && routers.size() > 0)
            {
               for(Outbound router: routers)
                  route(router, msgKeysValue,msg);
            }
            else
            {
               if (statsCollector != null) statsCollector.messageNotSent(msg);
               logger.warn("No router found for message type \""+ SafeString.valueOf(msg) + 
                     (msg != null ? "\" of type \"" + SafeString.valueOf(msg.getClass()) : "") + "\"");
            }
         }
         else
         {
            if (statsCollector != null) statsCollector.messageNotSent(msg);
            logger.warn("Null message key for \""+ SafeString.valueOf(msg) + 
                  (msg != null ? "\" of type \"" + SafeString.valueOf(msg.getClass()) : "") + "\"");
         }
      }
   }
   
   @Override
   public void clusterStateChanged(RoutingStrategy.Outbound outbound)
   {
      try
      {
         Collection<Destination> destinations = outbound.getKnownDestinations();
         if (destinations == null)
            return;
         for (Destination d : destinations)
         {
            try { senderFactory.stopDestination(d); }
            catch (Throwable th)
            {
               logger.error("Trouble shutting down the destination " + SafeString.objectDescription(d),th);
            }
         }
      }
      catch (Throwable th)
      {
         logger.error("Unknown exception trying to clean up after a cluster state change for "+ outbound.getClusterId() +  
               " from  " + currentClusterDefinition.getClusterId(),th);
      }
   }
   
   public void stop()
   {
      if (outboundManager != null)
      {
         try { outboundManager.stop(); }
         catch (Throwable th)
         {
            logger.error("Stopping the Outbound Manager " + SafeString.objectDescription(outboundManager) + " caused an exception:", th);
         }
      }
      
      // stop the MpClusterSession first so that ClusterRouters wont
      //  be notified after their stopped.
      try { if(mpClusterSession != null) mpClusterSession.stop(); }
      catch(Throwable th) 
      {
         logger.error("Stopping the cluster session " + SafeString.objectDescription(mpClusterSession) + " caused an exception:", th);
      }
   }

   /**
    * Returns whether or not the message was actually sent. Doesn't touch the statsCollector
    */
   public boolean route(Outbound strategyOutbound, Object key, Object message)
   {
      boolean messageFailed = true;
      Sender sender = null;
      try
      {
         Destination destination = strategyOutbound.selectDestinationForMessage(key, message);

         if (destination == null)
         {
            if (logger.isInfoEnabled())
               logger.info("Couldn't find a destination for " + SafeString.objectDescription(message));
            if (statsCollector != null) statsCollector.messageNotSent(message);
            return false;
         }

         sender = senderFactory.getSender(destination);
         if (sender == null)
            logger.error("Couldn't figure out a means to send " + SafeString.objectDescription(message) +
                  " to " + SafeString.valueOf(destination) + "");
         else
         {
            byte[] data = serializer.serialize(message);
            sender.send(data); // the sender is assumed to increment the stats collector.
            messageFailed = false;
         }
      }
      catch(DempsyException e)
      {
         logger.info("Failed to determine the destination for " + SafeString.objectDescription(message) + 
               " using the routing strategy " + SafeString.objectDescription(strategyOutbound),e);
      }
      catch (SerializationException e)
      {
         logger.error("Failed to serialize " + SafeString.objectDescription(message) + 
               " using the serializer " + SafeString.objectDescription(serializer),e);
      }
      catch (MessageTransportException e)
      {
         logger.warn("Failed to send " + SafeString.objectDescription(message) + 
               " using the sender " + SafeString.objectDescription(sender),e);
      }
      catch (Throwable e)
      {
         logger.error("Failed to send " + SafeString.objectDescription(message) + 
               " using the serializer " + SafeString.objectDescription(serializer) +
               "\" and using the sender " + SafeString.objectDescription(sender),e);
      }
      if (messageFailed)
         statsCollector.messageNotSent(message);
      return !messageFailed;
   }
   
   protected void getMessages(Object message, List<Object> messages)
   {
      if(message instanceof Iterable)
      {
         @SuppressWarnings("rawtypes")
         Iterator it = ((Iterable)message).iterator();
         while(it.hasNext())
            getMessages(it.next(), messages);
      }
      else
         messages.add(message);
   }
    
}
