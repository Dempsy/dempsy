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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.RoutingStrategy.Outbound.Coordinator;

/**
 * This Routing Strategy uses the {@link MpCluster} to negotiate with other instances in the 
 * cluster.
 */
public class DecentralizedRoutingStrategy implements RoutingStrategy
{
   private static final int resetDelay = 500;
   
   private static Logger logger = LoggerFactory.getLogger(DecentralizedRoutingStrategy.class);
   
   private int defaultTotalSlots;
   private int defaultNumNodes;
   
   public DecentralizedRoutingStrategy(int defaultTotalSlots, int defaultNumNodes)
   {
      this.defaultTotalSlots = defaultTotalSlots;
      this.defaultNumNodes = defaultNumNodes;
   }
   
   private class Outbound implements RoutingStrategy.Outbound, ClusterInfoWatcher
   {
      private AtomicReference<Destination[]> destinations = new AtomicReference<Destination[]>();
      private RoutingStrategy.Outbound.Coordinator coordinator;
      private ClusterInfoSession clusterSession;
      private ClusterId clusterId;
      private Set<Class<?>> messageTypesHandled = null;

      private ScheduledExecutorService scheduler = null;
      
      private Outbound(RoutingStrategy.Outbound.Coordinator coordinator, ClusterInfoSession cluster, ClusterId clusterId)
      {
         this.coordinator = coordinator;
         this.clusterSession = cluster;
         this.clusterId = clusterId;
         execSetupDestinations(false);
      }

      @Override
      public ClusterId getClusterId() { return clusterId; }

      @Override
      public Destination selectDestinationForMessage(Object messageKey, Object message) throws DempsyException
      {
         Destination[] destinationArr = destinations.get();
         if (destinationArr == null)
            throw new DempsyException("It appears the Outbound strategy for the message key " + 
                  SafeString.objectDescription(messageKey) + 
                  " is being used prior to initialization.");
         int calculatedModValue = Math.abs(messageKey.hashCode()%destinationArr.length);
         return destinationArr[calculatedModValue];
      }
      
      @Override
      public void process()
      {
         execSetupDestinations(true);
      }
      
      @Override
      public synchronized void stop()
      {
         if (scheduler != null)
            scheduler.shutdown();
         scheduler = null;
      }
      
      /**
       * This makes sure all of the destinations are full.
       */
      @Override
      public boolean completeInitialization()
      {
         Destination[] ds = destinations.get();
         if (ds == null)
            return false;
         for (Destination d : ds)
            if (d == null)
               return false;
         return true;
      }
      
      /**
       * This method is protected for testing purposes. Otherwise it would be private.
       * @return whether or not the setup was successful.
       */
      protected synchronized boolean setupDestinations()
      {
         try
         {
            if (logger.isTraceEnabled())
               logger.trace("Resetting Outbound Strategy for cluster " + clusterId);

            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            int newtotalAddressCounts = fillMapFromActiveSlots(slotNumbersToSlots,clusterSession,clusterId,this);
            if (newtotalAddressCounts == 0)
               logger.info("The cluster " + SafeString.valueOf(clusterId) + " seems to be missing from the cluster info manager (zookeeper).");
            
            if (newtotalAddressCounts > 0)
            {
               Destination[] newDestinations = new Destination[newtotalAddressCounts];
               for (Map.Entry<Integer,DefaultRouterSlotInfo> entry : slotNumbersToSlots.entrySet())
               {
                  DefaultRouterSlotInfo slotInfo = entry.getValue();
                  newDestinations[entry.getKey()] = slotInfo.getDestination();

                  // only register the very first time ... for now
                  if (messageTypesHandled == null)
                  {
                     messageTypesHandled = new HashSet<Class<?>>();
                     messageTypesHandled.addAll(slotInfo.getMessageClasses());
                     coordinator.registerOutbound(this, messageTypesHandled);
                  }
               }

               // now see if anything is changed.
               Destination[] oldDestinations = destinations.get();
               if (oldDestinations == null || !Arrays.equals(oldDestinations, newDestinations))
                  destinations.set(newDestinations);
            }
            else
               destinations.set(null);
            
            return destinations.get() != null;
         }
         catch(ClusterInfoException e)
         {
            destinations.set(null);
            logger.warn("Failed to set up the Outbound for " + clusterId, e);
         }
         catch (RuntimeException rte)
         {
            logger.error("Failed to set up the Outbound for " + clusterId, rte);
         }
         return false;
      }
      
      private void execSetupDestinations(final boolean fromProcess)
      {
         if (!setupDestinations())
         {
            synchronized(this)
            {
               if (scheduler == null)
                  scheduler = Executors.newScheduledThreadPool(1);
            
               scheduler.schedule(new Runnable(){
                  @Override
                  public void run()
                  {
                     if (!setupDestinations())
                     {
                        synchronized(Outbound.this)
                        {
                           if (scheduler != null)
                              scheduler.schedule(this, resetDelay, TimeUnit.MILLISECONDS);
                        }
                     }
                     else
                     {
                        synchronized(Outbound.this)
                        {
                           if (scheduler != null)
                           {
                              scheduler.shutdown();
                              scheduler = null;
                           }
                        }
                     }
                  }
               }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
      }
   } // end Outbound class definition
   
   private class Inbound implements RoutingStrategy.Inbound, ClusterInfoWatcher
   {
      private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

      private List<Integer> destinationsAcquired = new ArrayList<Integer>();
      private ClusterInfoSession cluster;
      private Collection<Class<?>> messageTypes;
      private Destination thisDestination;
      private ClusterId clusterId;
      private KeyspaceResponsibilityChangeListener listener;
      
      private Inbound(ClusterInfoSession cluster, ClusterId clusterId,
            Collection<Class<?>> messageTypes, Destination thisDestination,
            KeyspaceResponsibilityChangeListener listener)
      {
         this.listener = listener;
         this.cluster = cluster;
         this.messageTypes = messageTypes;
         this.thisDestination = thisDestination;
         this.clusterId = clusterId;
         acquireSlots(false);
      }

      @Override
      public void process()
      {
         acquireSlots(true);
      }
      
      private synchronized void acquireSlots(final boolean fromProcess)
      {
         boolean retry = true;
         
         try
         {
            if (logger.isTraceEnabled())
               logger.trace("Resetting Inbound Strategy for cluster " + clusterId);

            int minNodeCount = defaultNumNodes;
            int totalAddressNeeded = defaultTotalSlots;
            Random random = new Random();
            boolean moreResponsitiblity = false;
            boolean lessResponsitiblity = false;

            //==============================================================================
            // need to verify that the existing slots in destinationsAcquired are still ours
            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            fillMapFromActiveSlots(slotNumbersToSlots,cluster, clusterId,this);
            Collection<Integer> slotsToReaquire = new ArrayList<Integer>();
            for (Integer destinationSlot : destinationsAcquired)
            {
               // select the coresponding slot information
               DefaultRouterSlotInfo slotInfo = slotNumbersToSlots.get(destinationSlot);
               if (slotInfo == null || !thisDestination.equals(slotInfo.getDestination()))
                  slotsToReaquire.add(destinationSlot);
            }
            //==============================================================================

            //==============================================================================
            // Now re-acquire the potentially lost slots
            for (Integer slotToReaquire : slotsToReaquire)
            {
               if (!acquireSlot(slotToReaquire, totalAddressNeeded,
                     cluster, clusterId, messageTypes, thisDestination))
               {
                  logger.info("Cannot reaquire the slot " + slotToReaquire + " for the cluster " + clusterId);
                  // I need to drop the slot from my list of destinations
                  destinationsAcquired.remove(slotToReaquire);
                  lessResponsitiblity = true;
               }
            }
            //==============================================================================

            while(needToGrabMoreSlots(cluster,minNodeCount,totalAddressNeeded))
            {
               int randomValue = random.nextInt(totalAddressNeeded);
               if(destinationsAcquired.contains(randomValue))
                  continue;
               if (acquireSlot(randomValue, totalAddressNeeded,
                     cluster, clusterId, messageTypes, thisDestination))
               {
                  destinationsAcquired.add(randomValue);
                  moreResponsitiblity = true;
               }
            }
            
            retry = false;
            
            if (lessResponsitiblity || moreResponsitiblity)
               listener.keyspaceResponsibilityChanged(this,lessResponsitiblity, moreResponsitiblity);
         }
         catch(ClusterInfoException e)
         {
// I don't think I want to do this ... if I have a Cluster problem I want to continue on my merry way
//  until I have confirmation something dramatic has happened.
//            destinationsAcquired.clear();
         }
         finally
         {
            // if we never got the destinations set up then kick off a retry
            if (retry)
               scheduler.schedule(new Runnable(){
                  @Override
                  public void run() { acquireSlots(fromProcess); }
               }, resetDelay, TimeUnit.MILLISECONDS);
         }
      }
      
      @Override
      public void stop()
      {
         scheduler.shutdown();
      }
      
      private boolean needToGrabMoreSlots(ClusterInfoSession clusterHandle,
            int minNodeCount, int totalAddressNeeded) throws ClusterInfoException
      {
         int addressInUse = clusterHandle.getSubdirs(clusterId.asPath(), null).size();
         int maxSlotsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)minNodeCount);
         return addressInUse < totalAddressNeeded && destinationsAcquired.size() < maxSlotsForOneNode;
      }
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return destinationsAcquired.contains(messageKey.hashCode()%defaultTotalSlots);
      }

   } // end Inbound class definition
   
   @Override
   public RoutingStrategy.Inbound createInbound(ClusterInfoSession cluster, ClusterId clusterId,
         Collection<Class<?>> messageTypes, Destination thisDestination, Inbound.KeyspaceResponsibilityChangeListener listener)
   {
      return new Inbound(cluster,clusterId,messageTypes,thisDestination,listener);
   }

   @Override
   public RoutingStrategy.Outbound createOutbound(Coordinator coordinator, ClusterInfoSession cluster, ClusterId clusterId)
   {
      return new Outbound(coordinator,cluster,clusterId);
   }
   
   static class DefaultRouterSlotInfo extends SlotInformation
   {
      private static final long serialVersionUID = 1L;

      private int totalAddress = -1;
      private int slotIndex = -1;

      public int getSlotIndex() { return slotIndex; }
      public void setSlotIndex(int modValue) { this.slotIndex = modValue; }

      public int getTotalAddress() { return totalAddress; }
      public void setTotalAddress(int totalAddress) { this.totalAddress = totalAddress; }

      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = super.hashCode();
         result = prime * result + (int)(slotIndex ^ (slotIndex >>> 32));
         result = prime * result + (int)(totalAddress ^ (totalAddress >>> 32));
         return result;
      }

      @Override
      public boolean equals(Object obj)
      {
         if (!super.equals(obj))
            return false;
         DefaultRouterSlotInfo other = (DefaultRouterSlotInfo)obj;
         if(slotIndex != other.slotIndex)
            return false;
         if(totalAddress != other.totalAddress)
            return false;
         return true;
      }
   }
   
   static class DefaultRouterClusterInfo implements Serializable
   {
      private static final long serialVersionUID = 1L;

      private AtomicInteger minNodeCount = new AtomicInteger(5);
      private AtomicInteger totalSlotCount = new AtomicInteger(300);
      
      public DefaultRouterClusterInfo(int totalSlotCount, int nodeCount)
      {
         this.totalSlotCount.set(totalSlotCount);
         this.minNodeCount.set(nodeCount);
      }

      public int getMinNodeCount() { return minNodeCount.get(); }
      public void setMinNodeCount(int nodeCount) { this.minNodeCount.set(nodeCount); }

      public int getTotalSlotCount(){ return totalSlotCount.get();  }
      public void setTotalSlotCount(int addressMultiplier) { this.totalSlotCount.set(addressMultiplier); }
   }
   
   /**
    * Fill the map of slots to slotinfos for internal use. 
    * @return the totalAddressCount from each slot. These are supposed to be repeated.
    */
   private static int fillMapFromActiveSlots(Map<Integer,DefaultRouterSlotInfo> mapToFill, ClusterInfoSession session,
         ClusterId clusterId, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      int totalAddressCounts = -1;
      Collection<String> slotsFromClusterManager;
      try
      {
         slotsFromClusterManager = session.getSubdirs(clusterId.asPath(), watcher);
      }
      catch (ClusterInfoException.NoNodeException e)
      {
         // mkdir and retry
         session.mkdir("/" + clusterId.getApplicationName(),DirMode.PERSISTENT);
         session.mkdir(clusterId.asPath(),DirMode.PERSISTENT);
         slotsFromClusterManager = session.getSubdirs(clusterId.asPath(), watcher);
      }

      if(slotsFromClusterManager != null)
      {
         // zero is valid but we only want to set it if we are not 
         // going to enter into the loop below.
         if (slotsFromClusterManager.size() == 0)
            totalAddressCounts = 0;
         
         for(String node: slotsFromClusterManager)
         {
            DefaultRouterSlotInfo slotInfo = (DefaultRouterSlotInfo)session.getData(clusterId.asPath() + "/" + node, null);
            if(slotInfo != null)
            {
               mapToFill.put(slotInfo.getSlotIndex(), slotInfo);
               if (totalAddressCounts == -1)
                  totalAddressCounts = slotInfo.getTotalAddress();
               else if (totalAddressCounts != slotInfo.getTotalAddress())
                  logger.error("There is a problem with the slots taken by the cluster manager for the cluster " + 
                        clusterId + ". Slot " + slotInfo.getSlotIndex() +
                        " from " + SafeString.objectDescription(slotInfo.getDestination()) + 
                        " thinks the total number of slots for this cluster it " + slotInfo.getTotalAddress() +
                        " but a former slot said the total was " + totalAddressCounts);
            }
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireSlot(int slotNum, int totalAddressNeeded,
         ClusterInfoSession clusterHandle, ClusterId clusterId, 
         Collection<Class<?>> messagesTypes, Destination destination) throws ClusterInfoException
   {
      String slotPath = clusterId.asPath() + "/" + String.valueOf(slotNum);
      if (clusterHandle.mkdir(slotPath,DirMode.EPHEMERAL) != null)
      {
         DefaultRouterSlotInfo dest = (DefaultRouterSlotInfo)clusterHandle.getData(slotPath, null);
         if(dest == null)
         {
            dest = new DefaultRouterSlotInfo();
            dest.setDestination(destination);
            dest.setSlotIndex(slotNum);
            dest.setTotalAddress(totalAddressNeeded);
            dest.setMessageClasses(messagesTypes);
            clusterHandle.setData(slotPath, dest);
         }
         return true;
      }
      else
         return false;
   }

}
