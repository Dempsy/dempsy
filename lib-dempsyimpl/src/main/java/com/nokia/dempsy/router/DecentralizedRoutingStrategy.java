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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.nokia.dempsy.router.microshard.MicroShardUtils;

/**
 * This Routing Strategy uses the {@link MpCluster} to negotiate with other instances in the 
 * cluster.
 */
public class DecentralizedRoutingStrategy implements RoutingStrategy
{
   private static final int resetDelay = 500;
   
   private static Logger logger = LoggerFactory.getLogger(DecentralizedRoutingStrategy.class);
   
   protected int defaultTotalShards;
   protected int minNumberOfNodes;
   
   private ScheduledExecutorService scheduler_ = null;
   
   public DecentralizedRoutingStrategy(int defaultTotalShards, int minNumberOfNodes)
   {
      this.defaultTotalShards = defaultTotalShards;
      this.minNumberOfNodes = minNumberOfNodes;
   }
   
   public class OutboundManager implements RoutingStrategy.OutboundManager, ClusterInfoWatcher
   {
      private ClusterInfoSession session;
      private ClusterId clusterId;
      private Collection<ClusterId> explicitClusterDestinations;
      private ConcurrentHashMap<Class<?>, Set<RoutingStrategy.Outbound>> routerMap = new ConcurrentHashMap<Class<?>, Set<RoutingStrategy.Outbound>>();
      private Map<String,RoutingStrategy.Outbound> outboundsByCluster = new HashMap<String, RoutingStrategy.Outbound>();
      private Set<String> clustersToReconsult = new HashSet<String>();
      private MicroShardUtils msutils;
      private ClusterStateMonitor monitor = null;
      
      private OutboundManager(ClusterInfoSession cluster, ClusterId clusterId, Collection<ClusterId> explicitClusterDestinations)
      {
         this.clusterId = clusterId;
         this.session = cluster;
         this.explicitClusterDestinations = explicitClusterDestinations;
         this.msutils = new MicroShardUtils(clusterId);
         setup();
      }
      
      @Override
      public void setClusterStateMonitor(ClusterStateMonitor monitor) { this.monitor = monitor; }
      
      @Override
      public void process() { setup(); }
      
      /**
       * Stop the manager and all of the Outbounds
       */
      @Override
      public synchronized void stop()
      {
         // flatten out then stop all of the Outbounds
         Collection<RoutingStrategy.Outbound> routers = outboundsByCluster.values();
         routerMap = null;
         outboundsByCluster = null;
         for (RoutingStrategy.Outbound router : routers)
            ((Outbound)router).stop();
      }
      
      /**
       * Called only from tests.
       */
      public Collection<Class<?>> getTypesWithNoOutbounds()
      {
         Collection<Class<?>> ret = new HashSet<Class<?>>();
         for (Map.Entry<Class<?>, Set<RoutingStrategy.Outbound>> entry : routerMap.entrySet())
         {
            if (entry.getValue().size() == 0)
               ret.add(entry.getKey());
         }
         return ret;
      }
      
      private Set<RoutingStrategy.Outbound> routerMapGetAndPutIfAbsent(Class<?> messageType)
      {
         Set<RoutingStrategy.Outbound> tmp = Collections.newSetFromMap(new ConcurrentHashMap<RoutingStrategy.Outbound, Boolean>());
         Set<RoutingStrategy.Outbound> ret = routerMap.putIfAbsent(messageType,tmp);
         if (ret == null)
            ret = tmp;
         return ret;
      }
      
      private boolean isAssignableFrom(Collection<Class<?>> oneOfThese, Class<?> fromThis)
      {
         for (Class<?> clazz : oneOfThese)
         {
            if (clazz.isAssignableFrom(fromThis))
               return true;
         }
         return false;
      }
      
      private Set<RoutingStrategy.Outbound> getOutboundsFromType(Class<?> messageType)
      {
         Set<RoutingStrategy.Outbound> ret = routerMap.get(messageType);
         if (ret != null)
            return ret;
         
         // now see if any of the entries are subclasses and if so, make another entry
         synchronized(this)
         {
            for(Map.Entry<Class<?>,Set<RoutingStrategy.Outbound>> c: routerMap.entrySet())
            {
               if(c.getKey().isAssignableFrom(messageType))
               {
                  ret = c.getValue();
                  routerMap.put(messageType, ret);
                  break;
               }
            }
         }
         return ret;
      }
      
      // only called with the lock held.
      private void clearRouterMap()
      {
         Collection<RoutingStrategy.Outbound> obs = new HashSet<RoutingStrategy.Outbound>();
         obs.addAll(outboundsByCluster.values());
         routerMap.clear();
         outboundsByCluster.clear();
         for (RoutingStrategy.Outbound o : obs)
         {
            if (monitor != null)
               // TODO: be more efficient here and attempt to see which clusters
               // have actually changed
               monitor.clusterStateChanged(o);

            ((Outbound)o).stop();
         }
      }
      
      @Override
      public Collection<RoutingStrategy.Outbound> retrieveOutbounds(Class<?> messageType)
      {
         if (messageType == null)
            return null;
         
         // if we've seen this type before
         Set<RoutingStrategy.Outbound> ret = getOutboundsFromType(messageType);
         if (ret == null || ret.size() == 0)
         {
            // We need to check for any registered clusters that can handle this type.
            //
            // Any clusters without type information yet needs to be listened to until 
            // such a time as data is available and we can make a determination as to
            // whether or not the new cluster applies to any.
            //
            // An alternative is to simply skip those clusters until successive messages 
            // require additional checking. (I think I'm going with the alternative).
            //
            // we want to make sure only one thread reads the cluster info manager
            // and creates outbounds from the results, at a time.
            synchronized(this)
            {
               try
               {
                  Set<String> clustersThatSupportClasses = new HashSet<String>();
                  Collection<String> curClusters = session.getSubdirs(msutils.getAppDir(), this);
                  for (String cluster : curClusters)
                  {
                     // null as the cluster watcher assumes that the DefaultRouterClusterInfo cannot be changed
                     // if this returns null we have a problem.
                     DefaultRouterClusterInfo info = (DefaultRouterClusterInfo)session.getData(msutils.getAppDir() + "/" + cluster,null);
                     if (info == null)
                     {
                        // this is a bad situation and should be corrected eventually because this is the result
                        // of a race condition. The above 'getData' call happened right between the creation of 
                        // the cluster directory, and he putting of the data.
                        // In this case we should just pass on this cluster since it's clearly not ready yet.
                        // BUT it needs to be re-consulted.
                        synchronized(clustersToReconsult){ clustersToReconsult.add(cluster); }
                     }
                     else // this is the expected condition
                     {
                        if (isAssignableFrom(info.messageClasses,messageType))
                           // we found a cluster that should be added to our list
                           clustersThatSupportClasses.add(cluster);
                     }
                  } // end for loop over all known app clusters 
                     
                  // now we need to create an Outbound for each cluster
                  ret = routerMapGetAndPutIfAbsent(messageType);
                  
                  for (String cluster : clustersThatSupportClasses)
                  {
                     // we need to make sure either explicitClusterDestinations isn't set or it
                     // contains the cluster we're looking at here.
                     if (explicitClusterDestinations == null || explicitClusterDestinations.contains(cluster))
                     {
                        RoutingStrategy.Outbound outbound = outboundsByCluster.get(cluster);
                        if (outbound == null)
                        {
                           outbound = new Outbound(session, new ClusterId(clusterId.getApplicationName(),cluster));
                           RoutingStrategy.Outbound other = outboundsByCluster.put(cluster, outbound);
                           // this would be odd
                           if (other != null)
                              ((Outbound)other).stop();
                        }
                        ret.add(outbound);
                     }
                  }
               }
               catch (ClusterInfoException e)
               {
                  // looks like we lost this one ...
                  if (logger.isDebugEnabled())
                     logger.debug("Exception while trying to setup Outbound for a message of type \"" + messageType.getSimpleName() + "\"",e);
                  clearRouterMap();
                  ret = null; // no chance.
               }
            } // end synchronized(this)
         } // end if the routerMap didn't have any Outbounds that corespond to the given messageType
         
         if (clustersToReconsult.size() > 0)
            reconsultClusters();
         
         return ret;
      }
      
      private void reconsultClusters()
      {
         Collection<String> tmp;
         Collection<String> successful;
         
         synchronized(clustersToReconsult)
         {
            successful = new ArrayList<String>(clustersToReconsult.size());
            tmp = new ArrayList<String>(clustersToReconsult.size());
            tmp.addAll(clustersToReconsult);
         }
         
         synchronized(this)
         {
            for (String cluster : tmp)
            {
               // if we already know about this cluster then we can simply remove it 
               // from the clustersToReconsult
               if (outboundsByCluster.containsKey(cluster))
               {
                  successful.add(cluster);
                  continue;
               }
               
               try
               {
                  DefaultRouterClusterInfo info = (DefaultRouterClusterInfo)session.getData(msutils.getAppRootDir() + "/" + cluster, null);
                  if (info == null || info.messageClasses == null)
                     continue;
                  
                  for (Class<?> clazz : info.messageClasses)
                  {
                     Set<RoutingStrategy.Outbound> outbounds = routerMap.get(clazz);
                     if (outbounds != null) // then we care about this messageType
                     {
                        // since we know we care about this cluster (because the routerMap entry
                        //  that coresponds to it exists), we should now be able to create an Outbound
                        RoutingStrategy.Outbound ob = new Outbound(session, new ClusterId(clusterId.getApplicationName(),cluster));
                        outbounds.add(ob);
                        RoutingStrategy.Outbound other = outboundsByCluster.put(cluster, ob);
                        // this would be odd
                        if (other != null)
                           ((Outbound)other).stop();

                     }
                  }
                  successful.add(cluster);
               }
               catch (ClusterInfoException cie)
               {
                  // just skip it for now
               }
            }
         }
         
         synchronized(clustersToReconsult)
         {
            for (String cur : successful)
               clustersToReconsult.remove(cur);
         }

      }

      private synchronized void setup()
      {
         clearRouterMap(); // this will cause a nice redo over everything
      }

      public class Outbound implements RoutingStrategy.Outbound, ClusterInfoWatcher
      {
         private AtomicReference<Destination[]> destinations = new AtomicReference<Destination[]>();
         private ClusterInfoSession clusterSession;
         private ClusterId clusterId;
         private AtomicBoolean isRunning = new AtomicBoolean(true);

         private Outbound(ClusterInfoSession cluster, ClusterId clusterId)
         {
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
         public Collection<Destination> getKnownDestinations()
         {
            List<Destination> ret = new ArrayList<Destination>();
            Destination[] destinationArr = destinations.get();
            if (destinationArr == null)
               return ret; // I guess we don't know about any
            for (Destination d : destinationArr)
               if (d != null)
                  ret.add(d);
            return ret;
         }

         /**
          * Shut down and reclaim any resources associated with the {@link Outbound} instance.
          */
         public synchronized void stop()
         {
            isRunning.set(false);
            disposeOfScheduler();
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

         @Override
         public boolean equals(Object other) {  return clusterId.equals(((Outbound)other).clusterId); }
         
         @Override
         public int hashCode() { return clusterId.hashCode(); }

         /**
          * This method is protected for testing purposes. Otherwise it would be private.
          * @return whether or not the setup was successful.
          */
         protected synchronized boolean setupDestinations()
         {
            try
            {
               if (logger.isTraceEnabled())
                  logger.trace("Resetting Outbound Strategy for cluster " + clusterId + 
                        " from " + OutboundManager.this.clusterId + " in " + this);

               Map<Integer,DefaultRouterShardInfo> shardNumbersToShards = new HashMap<Integer,DefaultRouterShardInfo>();
               Collection<String> emptyShards = new ArrayList<String>();
               int newtotalAddressCounts = fillMapFromActiveShards(shardNumbersToShards,emptyShards,clusterSession,clusterId,null,this);
               
               // For now if we hit the race condition between when the target Inbound
               // has created the shard and when it assigns the shard info, we simply claim
               // we failed.
               if (newtotalAddressCounts < 0 || emptyShards.size() > 0)
                  return false;
               
               if (newtotalAddressCounts == 0)
                  logger.info("The cluster " + SafeString.valueOf(clusterId) + " doesn't seem to have registered any details.");

               if (newtotalAddressCounts > 0)
               {
                  Destination[] newDestinations = new Destination[newtotalAddressCounts];
                  for (Map.Entry<Integer,DefaultRouterShardInfo> entry : shardNumbersToShards.entrySet())
                  {
                     DefaultRouterShardInfo shardInfo = entry.getValue();
                     newDestinations[entry.getKey()] = shardInfo.getDestination();
                  }

                  destinations.set(newDestinations);
               }
               else
                  destinations.set(new Destination[0]);
               
               return destinations.get() != null;
            }
            catch(ClusterInfoException e)
            {
               destinations.set(null);
               logger.warn("Failed to set up the Outbound for " + clusterId + " from " + OutboundManager.this.clusterId, e);
            }
            catch (RuntimeException rte)
            {
               logger.error("Failed to set up the Outbound for " + clusterId + " from " + OutboundManager.this.clusterId, rte);
            }
            return false;
         }
         
         private void execSetupDestinations()
         {
            if (!setupDestinations() && isRunning.get())
            {
               synchronized(this)
               {
                  ScheduledExecutorService sched = getScheduledExecutor();
                  if (sched != null)
                  {
                     sched.schedule(new Runnable(){
                        @Override
                        public void run()
                        {
                           if (isRunning.get() && !setupDestinations())
                           {
                              ScheduledExecutorService sched = getScheduledExecutor();
                              if (sched != null)
                                 sched.schedule(this, resetDelay, TimeUnit.MILLISECONDS);
                           }
                           else
                              disposeOfScheduler();
                        }
                     }, resetDelay, TimeUnit.MILLISECONDS);
                  }
               }
            }
         }
      } // end Outbound class definition
   } // end OutboundManager
   
   
   private class Inbound implements RoutingStrategy.Inbound
   {
      private Set<Integer> destinationsAcquired = new HashSet<Integer>();
      private ClusterInfoSession session;
      private Destination thisDestination;
      private ClusterId clusterId;
      private KeyspaceResponsibilityChangeListener listener;
      private MicroShardUtils msutils;
      private DefaultRouterClusterInfo clusterInfo;
      private AtomicBoolean isInited = new AtomicBoolean(false);
      private AtomicBoolean isRunning = new AtomicBoolean(true);
      
      private Inbound(ClusterInfoSession cluster, ClusterId clusterId,
            Collection<Class<?>> messageTypes, Destination thisDestination,
            KeyspaceResponsibilityChangeListener listener)
      {
         this.listener = listener;
         this.session = cluster;
         this.thisDestination = thisDestination;
         this.clusterId = clusterId;
         this.msutils = new MicroShardUtils(clusterId);
         this.clusterInfo = new DefaultRouterClusterInfo(defaultTotalShards, minNumberOfNodes, messageTypes);
         shardChangeWatcher.process(); // this invokes the acquireShards logic
      }
      
      ClusterInfoWatcher shardChangeWatcher = new PersistentTask()
      {
         public boolean execute() throws Throwable
         {
            if (logger.isTraceEnabled())
               logger.trace("Resetting Inbound Strategy for cluster " + clusterId);

            isInited.set(false);

            Random random = new Random();
            boolean moreResponsitiblity = false;
            boolean lessResponsitiblity = false;

            //==============================================================================
            // need to verify that the existing shards in destinationsAcquired are still ours
            Map<Integer,DefaultRouterShardInfo> shardNumbersToShards = new HashMap<Integer,DefaultRouterShardInfo>();
            Collection<String> emptyShards = new HashSet<String>();
            fillMapFromActiveShards(shardNumbersToShards,emptyShards, session, clusterId, clusterInfo, this);
            Collection<Integer> shardsToReaquire = new ArrayList<Integer>();
            for (Integer destinationShard : destinationsAcquired)
            {
               // select the corresponding shard information
               DefaultRouterShardInfo shardInfo = shardNumbersToShards.get(destinationShard);
               if (shardInfo == null || !thisDestination.equals(shardInfo.getDestination()) || emptyShards.contains(Integer.toString(destinationShard)))
                  shardsToReaquire.add(destinationShard);
            }
            //==============================================================================

            //==============================================================================
            // Now re-acquire the potentially lost shards
            for (Integer shardToReaquire : shardsToReaquire)
            {
               if (!acquireShard(shardToReaquire, defaultTotalShards, session, clusterId, thisDestination))
               {
                  logger.info("Cannot reaquire the shard " + shardToReaquire + " for the cluster " + clusterId);
                  // I need to drop the shard from my list of destinations
                  destinationsAcquired.remove(shardToReaquire);
                  lessResponsitiblity = true;
               }
            }
            //==============================================================================

            while(needToGrabMoreShards(minNumberOfNodes,defaultTotalShards))
            {
               int randomValue = random.nextInt(defaultTotalShards);
               if(destinationsAcquired.contains(randomValue) || shardIsInTransition(randomValue))
                  continue;
               if (acquireShard(randomValue, defaultTotalShards, session, clusterId, thisDestination))
               {
                  destinationsAcquired.add(randomValue);
                  moreResponsitiblity = true;
               }
            }
               
            if (logger.isTraceEnabled())
               logger.trace("Succesfully reset Inbound Strategy for cluster " + clusterId);

            if (lessResponsitiblity || moreResponsitiblity)
               listener.keyspaceResponsibilityChanged(Inbound.this,lessResponsitiblity, moreResponsitiblity);
            
            return true;
         }
      };
      
      PersistentTask nodeChangeWatcher = new PersistentTask()
      {
         public boolean execute() throws Throwable
         {
            if (logger.isTraceEnabled())
               logger.trace("Nodes changed in cluster " + clusterId);
            
            return true;
         }
      };
      
      
      @Override
      public boolean isInitialized() { return isInited.get(); }
      
      @Override
      public void stop()
      {
         isRunning.set(false);
      }
      
      private int getCurrentNodeCount() throws ClusterInfoException
      {
         return session.getSubdirs(msutils.getNodesDir(), nodeChangeWatcher).size();
      }
      
      private boolean needToGrabMoreShards(int minNumberOfNodes, int totalAddressNeeded) throws ClusterInfoException
      {
         int addressInUse = session.getSubdirs(msutils.getShardsDir(), null).size();
         int currentNodeCount = getCurrentNodeCount();
         int maxShardsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)(currentNodeCount < minNumberOfNodes ? minNumberOfNodes : currentNodeCount));
         return addressInUse < totalAddressNeeded && destinationsAcquired.size() < maxShardsForOneNode;
      }
      
//      private boolean needToGiveUpMoreShards(int totalAddressNeeded) throws ClusterInfoException
//      {
//         int currentNodeCount = getCurrentNodeCount();
//         if (currentNodeCount == 0)
//            return false;
//         int maxShardsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)currentNodeCount);
//         return destinationsAcquired.size() > maxShardsForOneNode;
//      }
      
      private boolean shardIsInTransition(int shardNum) throws ClusterInfoException
      {
         // the path would be the 
         String shardDir = String.valueOf(shardNum);
         return session.getSubdirs(msutils.getTransistionDir(), null).contains(shardDir);
      }
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return destinationsAcquired.contains(Math.abs(messageKey.hashCode()%defaultTotalShards));
      }
      
      private abstract class PersistentTask implements ClusterInfoWatcher
      {
         private boolean alreadyHere = false;
         private boolean recurseAttempt = false;
         private ScheduledFuture<?> currentlyWaitingOn = null;
         
         /**
          * This should return <code>true</code> if the underlying execute was successful.
          * It will be recalled until it does.
          */
         public abstract boolean execute() throws Throwable;
         
         @Override public void process() { executeUntilWorks(); }

         private synchronized void executeUntilWorks()
         {
            if (!isRunning.get())
               return;
            
            boolean retry = true;
            
            try
            {
               // we need to flatten out recursions
               if (alreadyHere)
               {
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
               
               retry = !execute();
               
               if (logger.isTraceEnabled())
                  logger.trace("Nodes changed in cluster " + clusterId);
               
            }
            catch (Throwable th)
            {
               if (logger.isDebugEnabled())
                  logger.debug("Exception while " + this + " for " + clusterId, th);
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
                  ScheduledExecutorService sched = getScheduledExecutor();
                  if (sched != null)
                      currentlyWaitingOn = sched.schedule(new Runnable(){
                        @Override
                        public void run() { executeUntilWorks(); }
                     }, resetDelay, TimeUnit.MILLISECONDS);
               }
               else
               {
                  disposeOfScheduler();
                  isInited.set(true);
               }
            }
         }
      } // end PersistentTask abstract class definition
   } // end Inbound class definition
   
   @Override
   public RoutingStrategy.Inbound createInbound(ClusterInfoSession cluster, ClusterId clusterId,
         Collection<Class<?>> messageTypes, Destination thisDestination, Inbound.KeyspaceResponsibilityChangeListener listener)
   {
      return new Inbound(cluster,clusterId,messageTypes,thisDestination,listener);
   }
   
   @Override
   public RoutingStrategy.OutboundManager createOutboundManager(ClusterInfoSession session, 
         ClusterId clusterId, Collection<ClusterId> explicitClusterDestinations)
   {
      return new OutboundManager(session,clusterId,explicitClusterDestinations);
   }
   
   public static class DefaultRouterShardInfo
   {
      private int totalAddress = -1;
      private int shardIndex = -1;
      private Destination destination;

      public int getShardIndex() { return shardIndex; }
      public void setShardIndex(int modValue) { this.shardIndex = modValue; }

      public int getTotalAddress() { return totalAddress; }
      public void setTotalAddress(int totalAddress) { this.totalAddress = totalAddress; }
      
      public Destination getDestination() { return destination; }
      public void setDestination(Destination destination) { this.destination = destination; }

      @Override
      public String toString() { 
         return "{ shardIndex:" + shardIndex + ", totalAddress:" + totalAddress + ", destination:" + SafeString.objectDescription(destination) + "}";
      }
   }
   
   public static class DefaultRouterClusterInfo
   {
      private int minNodeCount = 5;
      private int totalShardCount = 300;
      private Set<Class<?>> messageClasses = new HashSet<Class<?>>();
      
      public DefaultRouterClusterInfo() {} // required for deserialize
      
      public DefaultRouterClusterInfo(int totalShardCount, int minNodeCount, Collection<Class<?>> messageClasses)
      {
         this.totalShardCount = totalShardCount;
         this.minNodeCount = minNodeCount;
         if (messageClasses != null)
            this.messageClasses.addAll(messageClasses);
      }

      public int getMinNodeCount() { return minNodeCount;  }
      public void setMinNodeCount(int minNodeCount) { this.minNodeCount = minNodeCount; }

      public int getTotalShardCount() { return totalShardCount; }
      public void setTotalShardCount(int totalShardCount) { this.totalShardCount = totalShardCount; }

      public Set<Class<?>> getMessageClasses() { return messageClasses;}
      public void setMessageClasses(Set<Class<?>> messageClasses) { this.messageClasses = messageClasses; }

      @Override
      public String toString() { return "{ minNodeCount:" + minNodeCount + ", totalShardCount:" + totalShardCount + ", messageClasses:" + messageClasses + "}"; }
   }
   
   /**
    * Fill the map of shards to shardinfos for internal use. 
    * 
    * @param mapToFill is the map to fill from the ClusterInfo.
    * @param emptyShards if not null, will have the shard path of any shards that exist but don't have 
    * {@link DefaultRouterShardInfo} set yet. This can happen when there is a race between creating the
    * EPHEMERAL directory for the shard but it's accessed from the fillMapFromActiveShards method
    * prior to the owner having set the data on it.
    * @param session is the ClusterInfoSession to get retrieve the data from.
    * @param clusterInfo is the {@link DefaultRouterClusterInfo} for the cluster we're retrieving the shards for.
    * @param watcher, if not null, will be set as the watcher on the shard directory for this cluster.
    * @return the totalAddressCount from each shard. These are supposed to be repeated in each 
    * {@link DefaultRouterShardInfo}.
    */
   private static int fillMapFromActiveShards(Map<Integer,DefaultRouterShardInfo> mapToFill, Collection<String> emptyShards, 
         ClusterInfoSession session, ClusterId clusterId, DefaultRouterClusterInfo clusterInfo, 
         ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      MicroShardUtils msutils = new MicroShardUtils(clusterId);
      int totalAddressCounts = -1;
      Collection<String> shardsFromClusterManager;
      try
      {
         shardsFromClusterManager = session.getSubdirs(msutils.getShardsDir(), watcher);
      }
      catch (ClusterInfoException.NoNodeException e)
      {
         // clusterInfo == null means that this is a passive call to fillMapFromActiveShards
         // and shouldn't create extraneous directories. If they are not there, there's
         // nothing we can do.
         if (clusterInfo != null)
         {
            msutils.mkAllPersistentAppDirs(session, clusterInfo);
            shardsFromClusterManager = session.getSubdirs(msutils.getShardsDir(), watcher);
         }
         else
            shardsFromClusterManager = null;
      }

      if(shardsFromClusterManager != null)
      {
         // zero is valid but we only want to set it if we are not 
         // going to enter into the loop below.
         if (shardsFromClusterManager.size() == 0)
            totalAddressCounts = 0;
         
         for(String node: shardsFromClusterManager)
         {
            DefaultRouterShardInfo shardInfo = (DefaultRouterShardInfo)session.getData(msutils.getShardsDir() + "/" + node, null);
            if(shardInfo != null)
            {
               mapToFill.put(shardInfo.getShardIndex(), shardInfo);
               if (totalAddressCounts == -1)
                  totalAddressCounts = shardInfo.getTotalAddress();
               else if (totalAddressCounts != shardInfo.getTotalAddress())
                  logger.error("There is a problem with the shards taken by the cluster manager for the cluster " + 
                        clusterId + ". Shard " + shardInfo.getShardIndex() +
                        " from " + SafeString.objectDescription(shardInfo.getDestination()) + 
                        " thinks the total number of shards for this cluster it " + shardInfo.getTotalAddress() +
                        " but a former shard said the total was " + totalAddressCounts);
            }
            else
            {
               if (emptyShards != null)
                  emptyShards.add(node);
               if (logger.isDebugEnabled())
                  logger.debug("Retrieved empty shard for cluster " + clusterId + ", shard number " + node);
            }
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireShard(int shardNum, int totalAddressNeeded,
         ClusterInfoSession clusterHandle, ClusterId clusterId, 
         Destination destination) throws ClusterInfoException
   {
      MicroShardUtils utils = new MicroShardUtils(clusterId);
      String shardPath = utils.getShardsDir() + "/" + String.valueOf(shardNum);
      if (clusterHandle.mkdir(shardPath,DirMode.EPHEMERAL) != null)
      {
         DefaultRouterShardInfo dest = (DefaultRouterShardInfo)clusterHandle.getData(shardPath, null);
         if(dest == null)
         {
            dest = new DefaultRouterShardInfo();
            dest.setDestination(destination);
            dest.setShardIndex(shardNum);
            dest.setTotalAddress(totalAddressNeeded);
            clusterHandle.setData(shardPath, dest);
         }
         return true;
      }
      else
         return false;
   }
   
   protected synchronized ScheduledExecutorService getScheduledExecutor()
   {
      if (scheduler_ == null)
         scheduler_ = Executors.newScheduledThreadPool(1);
      return scheduler_;
   }
   
   protected synchronized void disposeOfScheduler()
   {
      if (scheduler_ != null)
         scheduler_.shutdown();
      scheduler_ = null;
   }

}
