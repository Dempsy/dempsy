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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
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
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;
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
public class Router implements Dispatcher
{
   private static Logger logger = LoggerFactory.getLogger(Router.class);

   private AnnotatedMethodInvoker methodInvoker = new AnnotatedMethodInvoker(MessageKey.class);
   private ApplicationDefinition applicationDefinition = null;

   private ConcurrentHashMap<Class<?>, Set<ClusterRouter>> routerMap = new ConcurrentHashMap<Class<?>, Set<ClusterRouter>>();

   // protected for test access
   protected ConcurrentHashMap<Class<?>, Object> missingMsgTypes = new ConcurrentHashMap<Class<?>, Object>();
   private MpClusterSession<ClusterInformation, SlotInformation> mpClusterSession = null;
   private SenderFactory defaultSenderFactory;
   private ClusterId currentCluster = null;
   private StatsCollector statsCollector = null;
   
   protected Set<Class<?>> stopTryingToSendTheseTypes = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());
   
   public Router(ApplicationDefinition applicationDefinition) throws MpClusterException
   {
      if (applicationDefinition == null)
         throw new IllegalArgumentException("Can't pass a null applicationDefinition to a " + SafeString.valueOfClass(this));
      this.applicationDefinition = applicationDefinition;
   }

   /**
    * Provide the handle to the cluster factory so that each visible cluster can be reached.
    */
   public void setClusterSession(MpClusterSession<ClusterInformation, SlotInformation> factory) { mpClusterSession = factory; }
   
   /**
    * Tell the {@link Router} what the current cluster is. This is typically determined by
    * the {@link Dempsy} orchestrator through the use of the {@link CurrentClusterCheck}.
    */
   public void setCurrentCluster(ClusterId currentClusterId) { this.currentCluster = new ClusterId(currentClusterId); }
   
   /**
    * This sets the default {@link Transport} to use for each cluster a message may be routed to.
    * This can be overridden on a per-cluster basis.
    */
   public void setDefaultSenderFactory(SenderFactory senderFactory) { this.defaultSenderFactory = senderFactory; }
   
   /**
    * This sets the StatsCollector to log messages sent via this dispatcher to.
    */
   public void setStatsCollector(StatsCollector statsCollector) { this.statsCollector = statsCollector; }
   /**
    * Prior to the {@link Router} being used it needs to be initialized.
    */
   public void initialize() throws MpClusterException, DempsyException
   {
      // applicationDefinition cannot be null because the constructor checks
      
      // put all of the cluster definitions into a map for easy lookup
      Map<ClusterId, ClusterDefinition> defs = new HashMap<ClusterId, ClusterDefinition>();
      for (ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions())
         defs.put(clusterDef.getClusterId(), clusterDef);
      
      // now see about the one that we are.
      ClusterDefinition currentClusterDef = null;
      if (currentCluster != null)
      {
         currentClusterDef = defs.get(currentCluster);
         if (currentClusterDef == null)
            throw new DempsyException("This Dempsy instance seems to be misconfigured. While this VM thinks it's an instance of " +
                  currentCluster + " the application it's configured with doesn't contain this cluster definition. The application configuration consists of: " +
                  applicationDefinition);
      }

      // get the set of explicit destinations if they exist
      Set<ClusterId> explicitClusterDestinations = 
            (currentClusterDef != null && currentClusterDef.hasExplicitDestinations()) ? new HashSet<ClusterId>() : null;
      if (explicitClusterDestinations != null)
         explicitClusterDestinations.addAll(Arrays.asList(currentClusterDef.getDestinations()));
         

      // if the currentCluster is set and THAT cluster has explicit destinations
      //  then those are the only ones we want to consider
      for (ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions())
      {
         if (explicitClusterDestinations == null || explicitClusterDestinations.contains(clusterDef.getClusterId()))
         {
            ClusterRouter router = new ClusterRouter(clusterDef);
            router.setup(false);
         }
      }
   }

   /**
    * A {@link Router} is also a {@link Dispatcher} that is the instance that's typically
    * injected into {@link Adaptor}s. The implementation of this dispatch routes the message
    * to the appropriate {@link MessageProcessor} in the appropriate {@link ClusterDefinition}
    */
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
            Set<ClusterRouter> routers = getRouter(msg.getClass());
            if(routers != null)
            {
               for(ClusterRouter router: routers)
                  router.route(msgKeysValue,msg);
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
   
   public void stop()
   {
      // stop the MpClusterSession first so that ClusterRouters wont
      //  be notified after their stopped.
      try { if(mpClusterSession != null) mpClusterSession.stop(); }
      catch(Throwable th) 
      {
         logger.error("Stopping the cluster session " + SafeString.objectDescription(mpClusterSession) + " caused an exception:", th);
      }

      // flatten out then stop all of the ClusterRouters
      ConcurrentHashMap<Class<?>, Set<ClusterRouter>> map = routerMap;
      routerMap = null;
      Set<ClusterRouter> routers = new HashSet<ClusterRouter>();
      for (Collection<ClusterRouter> curRouters : map.values())
         routers.addAll(curRouters);
      for (ClusterRouter router : routers)
         router.stop();
   }
   
   /**
    * This class routes messages within a particular cluster. It is protected for test 
    * access only. Otherwise it would be private.
    */
   protected class ClusterRouter implements MpClusterWatcher<ClusterInformation,SlotInformation>
   {
      private Serializer<Object> serializer;
      private ClusterId clusterId;
      private MpCluster<ClusterInformation,SlotInformation> clusterHandle;
      private SenderFactory senderFactory = defaultSenderFactory;
      private volatile boolean isSetup = false;
      private RoutingStrategy strategy;
      private RoutingStrategy.Outbound strategyOutbound;
      
      @SuppressWarnings("unchecked")
      private ClusterRouter(ClusterDefinition clusterDef) throws MpClusterException, DempsyException
      {
         this.clusterId = new ClusterId(clusterDef.getClusterId());
         
         Object clusterRs = clusterDef.getRoutingStrategy(); 
         if (clusterRs == null)
            throw new DempsyException("Could not retrieve the routing strategy for " + SafeString.valueOf(clusterId));
         strategy = (RoutingStrategy)clusterRs;
         
         strategyOutbound = strategy.createOutbound();
         
         serializer = (Serializer<Object>)clusterDef.getSerializer();
         if (serializer == null)
            throw new DempsyException("Could not retrieve the serializer for " + SafeString.valueOf(clusterId));
      }
      
      @Override
      public void process(MpCluster<ClusterInformation,SlotInformation> cluster)
      {
         // it appears that the cluster configuration has changed.
         // we need to reset up the distributor
         try
         {
            setup(true);
         }
         catch(MpClusterException e)
         {
            logger.error("Major problem with the Router. Can't respond to changes in the cluster information:", e);
         }
      }

      public void route(Object key, Object message)
      {
         SlotInformation target = null;
         boolean messageFailed = true;
         Sender sender = null;
         try
         {
            target = strategyOutbound.selectSlotForMessageKey(key);
            if(target == null)
            {
               setup(false);
               target = strategyOutbound.selectSlotForMessageKey(key);
            }

            if(target != null)
            {
               Destination destination = target.getDestination();

               if (destination == null)
               {
                  logger.error("Couldn't find a destination for " + SafeString.objectDescription(message) + 
                        " from the cluster slot information selected " + SafeString.objectDescription(target));
                  return;
               }

               sender = senderFactory.getSender(destination);
               if (sender == null)
                  logger.error("Couldn't figure out a means to send " + SafeString.objectDescription(message) +
                        " to " + SafeString.valueOf(destination) + "");
               else
               {
                  byte[] data = serializer.serialize(message);
                  sender.send(data);
                  messageFailed = false;
                  if (statsCollector != null) statsCollector.messageSent(message);
               }

            }
            else
            {
               logger.warn("No destination found for the message " + SafeString.objectDescription(message) + 
                     " with the key " + SafeString.objectDescription(key));
            }
         }
         catch(DempsyException e)
         {
            logger.error("Failed to determine the destination for " + SafeString.objectDescription(message) + 
                  " using the routing strategy " + SafeString.objectDescription(strategy),e);
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
         finally
         {
            if (statsCollector != null && messageFailed) 
               statsCollector.messageFailed();
         }
      }
      
      public void setup(boolean force) throws MpClusterException
      {
         if (!isSetup || force)
         {
            synchronized(this)
            {
               if (!isSetup || force) // double checked locking
               {
                  isSetup = false;
                  clusterHandle = mpClusterSession.getCluster(clusterId);
                  clusterHandle.addWatcher(this);
                  strategyOutbound.resetCluster(clusterHandle);

                  Set<Class<?>> messageClasses = new HashSet<Class<?>>();

                  Collection<MpClusterSlot<SlotInformation>> nodes = clusterHandle.getActiveSlots();
                  if(nodes != null)
                  {
                     Set<Class<?>> msgClass = null;
                     for(MpClusterSlot<SlotInformation> node: nodes)
                     {
                        SlotInformation slotInfo = node.getSlotInformation();
                        if(slotInfo != null)
                        {
                           msgClass = slotInfo.getMessageClasses();
                           if (msgClass != null)
                              messageClasses.addAll(msgClass);
                        }
                     }
                     
                     // now we may have new messageClasses so we need to register with the Router
                     for (Class<?> clazz : messageClasses)
                     {
                        Set<ClusterRouter> cur = Collections.newSetFromMap(new ConcurrentHashMap<ClusterRouter, Boolean>()); // potential
                        Set<ClusterRouter> tmp = routerMap.putIfAbsent(clazz, cur);
                        if (tmp != null)
                           cur = tmp;
                        cur.add(this);
                     }

                  }
               }
            }
         }
      }
      
      private void stop()
      {
         try { if (senderFactory != null) senderFactory.stop(); }
         catch(Throwable th) 
         {
            logger.error("Stopping the sender factory " + SafeString.objectDescription(senderFactory) + " caused an exception:", th);
         }
      }
   } // end ClusterRouter definition.
   
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
    
   /**
    * This is protected for test access only. Otherwise it would be private.
    */
   protected Set<ClusterRouter> getRouter(Class<?> msgType)
   {
      Set<ClusterRouter> routers = routerMap.get(msgType);
      if(routers == null)
      {
         if(missingMsgTypes.contains(msgType))
            return null;
         else 
         {
            synchronized(routerMap)
            {
               routers = routerMap.get(msgType);
               if(routers == null)
               {
                  for(Class<?> c: routerMap.keySet())
                  {
                     if(c.isAssignableFrom(msgType))
                     {
                        routers = routerMap.get(c);
                        routerMap.put(msgType, routers);
                        break;
                     }
                  }
                  if(routers == null)
                     missingMsgTypes.put(msgType,new Object());
               }
            }
         }
      }
      
      return routers;
   }

}
