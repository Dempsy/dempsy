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
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;
import com.nokia.dempsy.router.RoutingStrategy.Outbound.Coordinator;

/**
 * This Routing Strategy uses the {@link MpCluster} to negotiate with other instances in the 
 * cluster.
 */
public class DefaultRoutingStrategy implements RoutingStrategy
{
   private static final int resetDelay = 500;
   
   private static Logger logger = LoggerFactory.getLogger(DefaultRoutingStrategy.class);
   
   private int defaultTotalSlots;
   private int defaultNumNodes;
   
   public DefaultRoutingStrategy(int defaultTotalSlots, int defaultNumNodes)
   {
      this.defaultTotalSlots = defaultTotalSlots;
      this.defaultNumNodes = defaultNumNodes;
   }
   
   private static class Outbound implements RoutingStrategy.Outbound, MpClusterWatcher
   {
      private AtomicReference<Destination[]> destinations = new AtomicReference<Destination[]>();
      private RoutingStrategy.Outbound.Coordinator coordinator;
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      private ClusterId clusterId;
      private Set<Class<?>> messageTypesHandled = null;

      private ScheduledExecutorService scheduler = null;

      private Outbound(RoutingStrategy.Outbound.Coordinator coordinator,
            MpCluster<ClusterInformation, SlotInformation> cluster)
      {
         this.coordinator = coordinator;
         this.cluster = cluster;
         this.clusterId = cluster.getClusterId();
         cluster.addWatcher(this);
         execSetupDestinations();
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
//         setupDestinations();
         execSetupDestinations();
      }
      
      @Override
      public synchronized void stop()
      {
         if (scheduler != null)
            scheduler.shutdown();
         scheduler = null;
      }
      
      private synchronized boolean setupDestinations()
      {
         try
         {
            if (logger.isTraceEnabled())
               logger.trace("Resetting Outbound Strategy for cluster " + clusterId);

            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            int newtotalAddressCounts = fillMapFromActiveSlots(slotNumbersToSlots,cluster);
            if (newtotalAddressCounts == 0)
               throw new MpClusterException("The cluster " + cluster.getClusterId() + 
                     " seems to have invalid slot information. Someone has set the total number of slots to zero.");
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
         catch(MpClusterException e)
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
      
      private void execSetupDestinations()
      {
         if (!setupDestinations())
         {
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
      
   } // end Outbound class definition
   
   private class Inbound implements RoutingStrategy.Inbound, MpClusterWatcher
   {
      private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

      private List<Integer> destinationsAcquired = new ArrayList<Integer>();
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      private Collection<Class<?>> messageTypes;
      private Destination thisDestination;
      private ClusterId clusterId;
      private AtomicInteger retries = new AtomicInteger(3);
      
      private Inbound(MpCluster<ClusterInformation, SlotInformation> cluster, 
            Collection<Class<?>> messageTypes,
            Destination thisDestination)
      {
         this.cluster = cluster;
         this.messageTypes = messageTypes;
         this.thisDestination = thisDestination;
         this.clusterId = cluster.getClusterId();
         process();
         this.cluster.addWatcher(this);
         this.retries.set(0);
      }

      @Override
      public synchronized void process()
      {
         boolean retry = true;
         try
         {
            if (logger.isTraceEnabled())
               logger.trace("Resetting Inbound Strategy for cluster " + clusterId);

            int minNodeCount = defaultNumNodes;
            int totalAddressNeeded = defaultTotalSlots;
            Random random = new Random();

            //==============================================================================
            // need to verify that the existing slots in destinationsAcquired are still ours
            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            fillMapFromActiveSlots(slotNumbersToSlots,cluster);
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
                     cluster, messageTypes, thisDestination))
               {
                  // in this case, see if I already own it...
                  logger.warn("Cannot reaquire the slot " + slotToReaquire + " for the cluster " + clusterId);
               }
            }
            //==============================================================================

            while(needToGrabMoreSlots(cluster,minNodeCount,totalAddressNeeded))
            {
               int randomValue = random.nextInt(totalAddressNeeded);
               if(destinationsAcquired.contains(randomValue))
                  continue;
               if (acquireSlot(randomValue, totalAddressNeeded,
                     cluster, messageTypes, thisDestination))
                  destinationsAcquired.add(randomValue);                  
            }
            retry = false;
         }
         catch(MpClusterException e)
         {
            destinationsAcquired.clear();
         }
         finally
         {
            // if we never got the destinations set up then kick off a retry
            if (retry && retries.get()>0)
            {
               retries.decrementAndGet();
               logger.error("Calling process again. retries left "+retries.get());
               process();
//               scheduler.schedule(new Runnable(){
//                  @Override
//                  public void run() { process(); }
//               }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
      }
      
      @Override
      public void stop()
      {
         scheduler.shutdown();
      }
      
      private boolean needToGrabMoreSlots(MpCluster<ClusterInformation, SlotInformation> clusterHandle,
            int minNodeCount, int totalAddressNeeded) throws MpClusterException
      {
         int addressInUse = clusterHandle.getActiveSlots().size();
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
   public RoutingStrategy.Inbound createInbound(MpCluster<ClusterInformation, SlotInformation> cluster, 
         Collection<Class<?>> messageTypes,
         Destination thisDestination)
   {
      return new Inbound(cluster,messageTypes,thisDestination);
   }

   @Override
   public RoutingStrategy.Outbound createOutbound(Coordinator coordinator, MpCluster<ClusterInformation, SlotInformation> cluster)
   {
      return new Outbound(coordinator,cluster);
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
   
   static class DefaultRouterClusterInfo extends ClusterInformation
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
   private static int fillMapFromActiveSlots(Map<Integer,DefaultRouterSlotInfo> mapToFill, 
         MpCluster<ClusterInformation, SlotInformation> clusterHandle)
   throws MpClusterException
   {
      int totalAddressCounts = -1;
      Collection<MpClusterSlot<SlotInformation>> slotsFromClusterManager = clusterHandle.getActiveSlots();

      if(slotsFromClusterManager != null)
      {
         for(MpClusterSlot<SlotInformation> node: slotsFromClusterManager)
         {
            DefaultRouterSlotInfo slotInfo = (DefaultRouterSlotInfo)node.getSlotInformation();
            if(slotInfo != null)
            {
               mapToFill.put(slotInfo.getSlotIndex(), slotInfo);
               if (totalAddressCounts == -1)
                  totalAddressCounts = slotInfo.getTotalAddress();
               else if (totalAddressCounts != slotInfo.getTotalAddress())
                  logger.error("There is a problem with the slots taken by the cluster manager for the cluster " + 
                        clusterHandle.getClusterId() + ". Slot " + slotInfo.getSlotIndex() +
                        " from " + SafeString.objectDescription(slotInfo.getDestination()) + 
                        " thinks the total number of slots for this cluster it " + slotInfo.getTotalAddress() +
                        " but a former slot said the total was " + totalAddressCounts);
            }
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireSlot(int slotNum, int totalAddressNeeded,
         MpCluster<ClusterInformation, SlotInformation> clusterHandle,
         Collection<Class<?>> messagesTypes, Destination destination) throws MpClusterException
   {
      MpClusterSlot<SlotInformation> slot = clusterHandle.join(String.valueOf(slotNum));
      if(slot == null)
         return false;
      DefaultRouterSlotInfo dest = (DefaultRouterSlotInfo)slot.getSlotInformation();
      if(dest == null)
      {
         dest = new DefaultRouterSlotInfo();
         dest.setDestination(destination);
         dest.setSlotIndex(slotNum);
         dest.setTotalAddress(totalAddressNeeded);
         dest.setMessageClasses(messagesTypes);
         slot.setSlotInformation(dest);
      }
      return true;
   }

}
