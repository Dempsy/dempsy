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
import java.util.concurrent.ScheduledFuture;
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
   
   private final int defaultTotalSlots;
   private final int defaultNumNodes;
   
   public DecentralizedRoutingStrategy(int defaultTotalSlots, int defaultNumNodes)
   {
      this.defaultTotalSlots = defaultTotalSlots;
      this.defaultNumNodes = defaultNumNodes;
   }
   
   private class Outbound implements RoutingStrategy.Outbound, ClusterInfoWatcher
   {
      // This should be 'set' ONLY in setDestinatios. Id this changes then
      //  we need to verify that cleanup is doe correctly
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
                  SafeString.objectDescription(messageKey) + " is being used prior to initialization.");
         int length = destinationArr.length;
         if (length == 0)
            return null;
         int calculatedModValue = Math.abs(messageKey.hashCode()%length);
         return destinationArr[calculatedModValue];
      }
      
      @Override
      public void process()
      {
         execSetupDestinations();
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
         return ds.length != 0; // this method is only called in tests and this needs to be true there.
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
               logger.info("The cluster " + SafeString.valueOf(clusterId) + " doesn't seem to have registered any details yet.");
            
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

               setDestinations(newDestinations);
            }
            else
               setDestinations(new Destination[0]);
            
            return destinations.get() != null;
         }
         catch(ClusterInfoException e)
         {
            setDestinations(null);
            logger.warn("Failed to set up the Outbound for " + clusterId, e);
         }
         catch (Throwable rte)
         {
            logger.error("Failed to set up the Outbound for " + clusterId, rte);
         }
         return false;
      }
      
      private final synchronized void setDestinations(Destination[] newDestinations)
      {
         // see what we're deleting so we can indicate to the Coordinator that
         // a particular destination is no longer needed by this Outbound
         Destination[] curDests = destinations.get();
         if (curDests != null && curDests.length > 0)
         {
            List<Destination> ndests = newDestinations == null ? new ArrayList<Destination>(0) : Arrays.asList(newDestinations);
            List<Destination> cdests = Arrays.asList(curDests);
            for (Destination odest : cdests)
            {
               if (odest != null && !ndests.contains(odest))
                  coordinator.finishedDestination(this,odest);
            }
         }
         destinations.set(newDestinations);
      }
      
      private void execSetupDestinations()
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
   
   class Inbound implements RoutingStrategy.Inbound, ClusterInfoWatcher
   {
      private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

      private final boolean[] destinationsAcquiredLookup = new boolean[defaultTotalSlots];
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
      
      boolean alreadyHere = false;
      boolean recurseAttempt = false;
      ScheduledFuture<?> currentlyWaitingOn = null;
      
      private synchronized void acquireSlots(final boolean fromProcess)
      {
         boolean retry = true;
         
         try
         {
            // we need to flatten out recursions
            if (alreadyHere)
            {
               logger.trace("Recurse attempt. Delaying call.");
               recurseAttempt = true;
               return;
            }
            alreadyHere = true;
            
            // ok ... we're going to execute this now. So if we have an outstanding scheduled task we
            // need to cancel it.
            if (currentlyWaitingOn != null)
            {
               currentlyWaitingOn.cancel(false);
               currentlyWaitingOn = null;
            }
            
            if (logger.isTraceEnabled())
               logger.trace("Resetting Inbound Strategy for cluster " + clusterId);
            
            final int minNodeCount = defaultNumNodes;
            final int totalAddressNeeded = defaultTotalSlots;
            final Random random = new Random();
            
            // Get the current state of the entire cluster.
            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            fillMapFromActiveSlots(slotNumbersToSlots, cluster, clusterId, this);
            
            // Create the set of slots that I already own.
            Set<Integer> shardsIOwn = new HashSet<Integer>(); 
            for (Map.Entry<Integer, DefaultRouterSlotInfo> entry: slotNumbersToSlots.entrySet())
            {
               if (thisDestination.equals(entry.getValue().getDestination()))
                  shardsIOwn.add(entry.getKey());
            }

            //==============================================================================
            // need to verify that the existing slots in destinationsAcquired are still ours
            // and re-acquire the potentially lost ones
            for (int index = 0; index < totalAddressNeeded; index++)
            {
               if (destinationsAcquiredLookup[index])
               {
                  // select the corresponding slot information
                  if(!shardsIOwn.contains(index))
                  {
                       if (acquireSlot(index, totalAddressNeeded,
                             cluster, clusterId, messageTypes, thisDestination))
                          shardsIOwn.add(index);
                       else
                           logger.info("Cannot reaquire the slot " + index + " for the cluster " + clusterId);
                  }
               }
            }
            //==============================================================================

            while(needToGrabMoreSlots(clusterId.asPath(),cluster,shardsIOwn.size(),minNodeCount,totalAddressNeeded))
            {
               int randomValue = random.nextInt(totalAddressNeeded);
               if(destinationsAcquiredLookup[randomValue])
                  continue;
               if (acquireSlot(randomValue, totalAddressNeeded,
                     cluster, clusterId, messageTypes, thisDestination))
                  shardsIOwn.add(randomValue);
            }
            
            //===============================================================================
            // It's critical that this happen without an exception. We are going to now update the
            // underlying destinationsAcquiredLookup array and once an entry is changed we must get to the 
            // keyspaceResponsibilityChanged call or we will have missed either MP evictions
            // or MP instantiations.
            boolean moreResponsitiblity = false;
            boolean lessResponsitiblity = false;
            for (int i = 0; i < destinationsAcquiredLookup.length; i++)
            {
               if (destinationsAcquiredLookup[i] == false && shardsIOwn.contains(i))
               {
                  destinationsAcquiredLookup[i] = true;
                  moreResponsitiblity = true;
               }
               else if (destinationsAcquiredLookup[i] == true && !shardsIOwn.contains(i) )
               {
                  destinationsAcquiredLookup[i] = false;
                  lessResponsitiblity = true;
               }
            }
            listener.keyspaceResponsibilityChanged(this,lessResponsitiblity, moreResponsitiblity);

            retry = false;
            
            if (logger.isTraceEnabled())
               logger.trace("Succesfully reset Inbound Strategy for cluster " + clusterId);
         }
         catch(ClusterInfoException e)
         {
            if (logger.isDebugEnabled())
               logger.debug("Exception while acquiring micro-shards for " + clusterId, e);
         }
         catch(Throwable th)
         {
            logger.error("Unexpected error resetting Inbound strategy",th);
         }
         finally
         {
            // if we never got the destinations set up then kick off a retry
            if (recurseAttempt)
               retry = true;
            
            recurseAttempt = false;
            alreadyHere = false;
            
            if (retry)
            {
               currentlyWaitingOn = scheduler.schedule(new Runnable(){
                  @Override
                  public void run() 
                  {
                     logger.trace("Kicking off scheduled acquireSlots");
                     acquireSlots(fromProcess);
                  }
               }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
      }
      
      @Override
      public void stop()
      {
         scheduler.shutdown();
      }
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return destinationsAcquiredLookup[Math.abs(messageKey.hashCode()%defaultTotalSlots)];
      }

   } // end Inbound class definition
   
   private static boolean needToGrabMoreSlots(String shardPath,
         ClusterInfoSession clusterHandle, int numIOwn,
         int minNodeCount, int totalAddressNeeded) throws ClusterInfoException
   {
      int addressInUse = clusterHandle.getSubdirs(shardPath, null).size();
      int maxSlotsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)minNodeCount);
      return addressInUse < totalAddressNeeded && numIOwn < maxSlotsForOneNode;
   }
   
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
   
   static public class DefaultRouterSlotInfo extends SlotInformation
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
   private static int fillMapFromActiveSlots(Map<Integer,DefaultRouterSlotInfo> mapToFill, 
         ClusterInfoSession session, ClusterId clusterId, ClusterInfoWatcher watcher) throws ClusterInfoException
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
         session.mkdir("/" + clusterId.getApplicationName(),null,DirMode.PERSISTENT);
         session.mkdir(clusterId.asPath(),null,DirMode.PERSISTENT);
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
            else
               throw new ClusterInfoException("There is an empty shard directory at " + clusterId.asPath() + "/" + node + " which ought to be impossible!");
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireSlot(int slotNum, int totalAddressNeeded,
         ClusterInfoSession clusterHandle, ClusterId clusterId, 
         Collection<Class<?>> messagesTypes, Destination destination) throws ClusterInfoException
   {
      String slotPath = clusterId.asPath() + "/" + String.valueOf(slotNum);
      DefaultRouterSlotInfo dest = new DefaultRouterSlotInfo();
      dest.setDestination(destination);
      dest.setSlotIndex(slotNum);
      dest.setTotalAddress(totalAddressNeeded);
      dest.setMessageClasses(messagesTypes);
      return clusterHandle.mkdir(slotPath,dest,DirMode.EPHEMERAL) != null;
   }

}
