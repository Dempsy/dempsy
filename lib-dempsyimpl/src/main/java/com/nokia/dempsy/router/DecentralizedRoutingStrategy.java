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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import com.nokia.dempsy.util.AutoDisposeSingleThreadScheduler;
import com.nokia.dempsy.util.Pair;
import com.nokia.dempsy.util.PersistentTask;

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
   
   private final AutoDisposeSingleThreadScheduler dscheduler = 
         new AutoDisposeSingleThreadScheduler(DecentralizedRoutingStrategy.class.getSimpleName() + " Disposable Scheduler");
   
   public static final class ShardInfo
   {
      final int shard;
      
      public ShardInfo(int shard) { this.shard = shard; }
      
      public final int getShard() { return shard; }
   }
   
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
      protected Collection<Class<?>> getTypesWithNoOutbounds()
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
         } // end if the routerMap didn't have any Outbounds that correspond to the given messageType
         
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

      public class Outbound implements RoutingStrategy.Outbound
      {
         private AtomicReference<Destination[]> destinations = new AtomicReference<Destination[]>();
         private ClusterInfoSession clusterSession;
         private ClusterId clusterId;
         private AtomicBoolean isRunning = new AtomicBoolean(true);

         private Outbound(ClusterInfoSession cluster, ClusterId clusterId)
         {
            this.clusterSession = cluster;
            this.clusterId = clusterId;
            setupDestinations.process();
         }

         @Override
         public ClusterId getClusterId() { return clusterId; }

         @Override
         public Pair<Destination,Object> selectDestinationForMessage(Object messageKey, Object message) throws DempsyException
         {
            Destination[] destinationArr = destinations.get();
            if (destinationArr == null)
               throw new DempsyException("It appears the Outbound strategy for the message key " + 
                     SafeString.objectDescription(messageKey) + " is being used prior to initialization.");
            int length = destinationArr.length;
            if (length == 0)
               return null;
            int calculatedModValue = Math.abs(messageKey.hashCode()%length);
            return new Pair<Destination,Object>(destinationArr[calculatedModValue],new ShardInfo(calculatedModValue));
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

         private PersistentTask setupDestinations = new PersistentTask(logger,isRunning,dscheduler,resetDelay)
         {
            public synchronized boolean execute()
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
         }; // end setupDestinations PersistentTask declaration
      } // end Outbound class definition
   } // end OutboundManager
   
   // public for testing
   public class Inbound implements RoutingStrategy.Inbound
   {
      // destinationsAcquired should only be modified through the modifyDestinationsAcquired method.
      private Set<Integer> destinationsAcquired = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
      
      private ClusterInfoSession session;
      private Destination thisDestination;
      private ClusterId clusterId;
      private KeyspaceResponsibilityChangeListener listener;
      private MicroShardUtils msutils;
      private DefaultRouterClusterInfo clusterInfo;
      private AtomicBoolean isRunning = new AtomicBoolean(true);
      
      private void modifyDestinationsAcquired(Collection<Integer> toRemove, Collection<Integer> toAdd)
      {
         if (logger.isTraceEnabled())
            logger.trace(toString() + " reconfiguring with toAdd:" + toAdd + ", toRemove:" + toRemove);
         if (toRemove != null) destinationsAcquired.removeAll(toRemove);
         if (toAdd != null) destinationsAcquired.addAll(toAdd);
         if (logger.isTraceEnabled())
            logger.trace(toString() + "<- now looks like");
      }
      
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
         this.listener.setInboundStrategy(this);
         shardChangeWatcher.process(); // this invokes the acquireShards logic
      }
      
      @Override
      public String toString() { return "Inbound " + thisDestination + clusterId + " owning " + destinationsAcquired ; }
      
      private String nodeDirectory = null;
      
      //==============================================================================
      // This PersistentTask watches the shards directory for chagnes and will make 
      // make sure that the 
      //==============================================================================
      ClusterInfoWatcher shardChangeWatcher = new PersistentTask(logger, isRunning, dscheduler, resetDelay)
      {
         @Override
         public String toString() { return "determine the shard distribution and acquire new ones or relinquish some as necessary for " + clusterId; }
         
         private final void checkNodeDirectory() throws ClusterInfoException
         {
            try
            {
               Collection<String> nodeDirs = persistentGetMainDirSubdirs(session, msutils, msutils.getClusterNodesDir(), this, clusterInfo);
               if (nodeDirectory == null || !session.exists(nodeDirectory, this))
               {
                  nodeDirectory = session.mkdir(msutils.getClusterNodesDir() + "/node_", DirMode.EPHEMERAL_SEQUENTIAL);
                  nodeDirs = session.getSubdirs(msutils.getClusterNodesDir(), this);
               }

               Destination curDest = (Destination)session.getData(nodeDirectory, null);
               if (curDest == null)
                  session.setData(nodeDirectory, thisDestination);
               else if (!thisDestination.equals(curDest)) // wth?
               {
                  String tmp = nodeDirectory;
                  nodeDirectory = null;
                  throw new ClusterInfoException("Impossible! The Node directory " + tmp + " contains the destination for " + curDest + " but should have " + thisDestination);
               }

               for (String subdir : nodeDirs)
               {
                  String fullPathToSubdir = msutils.getClusterNodesDir() + "/" + subdir;
                  curDest = (Destination)session.getData(fullPathToSubdir, null);
                  if (thisDestination.equals(curDest) && !fullPathToSubdir.equals(nodeDirectory)) // this is bad .. clean up
                     session.rmdir(fullPathToSubdir);
               }
            }
            catch (ClusterInfoException cie)
            {
               cleanupAfterExceptionDuringNodeDirCheck();
               throw cie;
            }
            catch (RuntimeException re)
            {
               cleanupAfterExceptionDuringNodeDirCheck();
               throw re;
            }
         }
         
         // called from the catch clauses in checkNodeDirectory
         private final void cleanupAfterExceptionDuringNodeDirCheck()
         {
            if (nodeDirectory != null)
            {
               // attempt to remove the node directory
               try 
               {
                  if (session.exists(nodeDirectory,null))
                     session.rmdir(nodeDirectory);
                  nodeDirectory = null;
               }
               catch (ClusterInfoException cie2) {}
            }
         }
         
         @Override
         public boolean execute() throws Throwable
         {
            boolean traceOn = logger.isTraceEnabled();
            if (traceOn)
               logger.trace(Inbound.this.toString() + "Resetting Inbound Strategy.");

            Random random = new Random();
            
            // check node directory
            checkNodeDirectory();
            
            int currentWorkingNodeCount = findWorkingNodeCount(session, msutils, minNumberOfNodes, this);
            int acquireUpToThisMany = (int)Math.floor((double)defaultTotalShards/(double)currentWorkingNodeCount);
            int releaseDownToThisMany = (int)Math.ceil((double)defaultTotalShards/(double)currentWorkingNodeCount);

            // we are rebalancing the shards so we will figure out what we are removing
            // and adding.
            Set<Integer> destinationsToRemove = new HashSet<Integer>();
            Set<Integer> destinationsToAdd = new HashSet<Integer>();
            
            //==============================================================================
            // need to verify that the existing shards in destinationsAcquired are still ours. 
            Map<Integer,DefaultRouterShardInfo> shardNumbersToShards = new HashMap<Integer,DefaultRouterShardInfo>();
            Collection<String> emptyShards = new HashSet<String>();
            fillMapFromActiveShards(shardNumbersToShards,emptyShards,session, clusterId, clusterInfo, this);

            // First, are there any I don't know about that are in shardNumbersToShards.
            // This could be because I was assigned a shard (or in a previous execute, I acquired one
            // but failed prior to accounting for it). In this case there will be shards in 
            //  shardNumbersToShards that are assigned to me but aren't in destinationsAcquired.
            //
            // We are also piggy-backing off this loop to count the numberOfShardsWeActuallyHave
            int numberOfShardsWeActuallyHave = 0;
            for (Map.Entry<Integer,DefaultRouterShardInfo> entry : shardNumbersToShards.entrySet())
            {
               if (thisDestination.equals(entry.getValue().getDestination()))
               {
                  numberOfShardsWeActuallyHave++; // this entry is a shard that's assigned to us
                  final Integer shardNumber = entry.getKey();
                  if (!destinationsAcquired.contains(shardNumber)) // if we never saw it then we need to take it.
                     destinationsToAdd.add(shardNumber); 
               }
            }
            
            if (traceOn)
               logger.trace("" + Inbound.this + " has these destinations that I didn't know about " + destinationsToAdd);
            
            // Now we are going to go through what we think we have and see if any are missing.
            Collection<Integer> shardsToReaquire = new ArrayList<Integer>();
            for (Integer destinationShard : destinationsAcquired)
            {
               // select the corresponding shard information
               DefaultRouterShardInfo shardInfo = shardNumbersToShards.get(destinationShard);
               if (shardInfo == null || !thisDestination.equals(shardInfo.getDestination()) || emptyShards.contains(Integer.toString(destinationShard)))
                  shardsToReaquire.add(destinationShard);
            }
            
            if (traceOn)
               logger.trace(Inbound.this.toString() + " has these destiantions that it seemed to have lost " + shardsToReaquire);
            //==============================================================================
            
            //==============================================================================
            // Now re-acquire the potentially lost shards.
            for (Integer shardToReaquire : shardsToReaquire)
            {
               // if we already have too many shards then there's no point in trying
               // to reacquire the the shard.
               if (numberOfShardsWeActuallyHave >= acquireUpToThisMany)
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " removing " + shardToReaquire + 
                           " from one's I care about because I already have " + numberOfShardsWeActuallyHave +
                           " but only need " + acquireUpToThisMany);
                  
                  destinationsToRemove.add(shardToReaquire); // we're going to skip it ... and drop it.
               }
               // otherwise we will try to reacquire it.
               else if (!acquireShard(shardToReaquire, defaultTotalShards, session, clusterId, thisDestination, shardNumbersToShards))
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " removing " + shardToReaquire + 
                           " from one's I care about because I couldn't reaquire it.");

                  logger.info("Cannot reaquire the shard " + shardToReaquire + " for the cluster " + clusterId);
                  // I need to drop the shard from my list of destinations
                  destinationsToRemove.add(shardToReaquire);
               }
               else // otherwise, we successfully reacquired it.
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " reacquired " + shardToReaquire);
                  
                  numberOfShardsWeActuallyHave++; // we have one more.
               }
            }
            //==============================================================================
            
            //==============================================================================
            // Here, if we have too many shards, we will give up anything in the list destinationsToAdd
            // until we are either at the level were we should be, or we have no more to give up
            // from the list of those we were planning on adding.
            Iterator<Integer> curPos = destinationsToAdd.iterator();
            while (numberOfShardsWeActuallyHave > releaseDownToThisMany && 
                  destinationsToAdd.size() > 0 && curPos.hasNext())
            {
               Integer cur = curPos.next();
               if (doIOwnThisShard(cur,shardNumbersToShards))
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " removing shard " + cur + " because I already have " + 
                           numberOfShardsWeActuallyHave + " but can give up to " + releaseDownToThisMany + " and was planning on adding it.");
                  
                  curPos.remove();
                  session.rmdir(msutils.getShardsDir() + "/" + cur);
                  numberOfShardsWeActuallyHave--;
                  shardNumbersToShards.remove(cur);
               }
            }
            //==============================================================================
            
            //==============================================================================
            // Here, if we still have too many shards, we will begin deleting destinationsToRemove
            // that we may own actually own.
            curPos = destinationsToRemove.iterator();
            while (numberOfShardsWeActuallyHave > releaseDownToThisMany && 
                  destinationsToRemove.size() > 0 && curPos.hasNext())
            {
               Integer cur = curPos.next();
               if (doIOwnThisShard(cur,shardNumbersToShards))
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " removing shard " + cur + " because I already have " + 
                           numberOfShardsWeActuallyHave + " but can give up to " + releaseDownToThisMany + 
                           " and was planning on removing it anyway.");

                  session.rmdir(msutils.getShardsDir() + "/" + cur);
                  numberOfShardsWeActuallyHave--;
                  shardNumbersToShards.remove(cur);
               }
            }
            //==============================================================================
            
            //==============================================================================
            // above we bled off the destinationsToAdd. Now we remove actually known destinationsAcquired
            Iterator<Integer> destinationsAcquiredIter = destinationsAcquired.iterator();
            while (numberOfShardsWeActuallyHave > releaseDownToThisMany && destinationsAcquiredIter.hasNext())
            {
               Integer cur = destinationsAcquiredIter.next();
               // if we're already set to remove it because it didn't appear in the initial fillMapFromActiveShards
               // then there's no need to remove it from the session as it's already gone.
               if (doIOwnThisShard(cur,shardNumbersToShards))
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " removing shard " + cur + " because I already have " + 
                           numberOfShardsWeActuallyHave + " but can give up to " + releaseDownToThisMany + ".");

                  session.rmdir(msutils.getShardsDir() + "/" + cur);
                  numberOfShardsWeActuallyHave--;
                  destinationsToRemove.add(cur);
                  shardNumbersToShards.remove(cur);
               }
            }
            //==============================================================================
            
            //==============================================================================
            // Now see if we need to grab more shards. Maybe we just came off backup or, in
            // the case of elasticity, maybe another node went down.
            if (traceOn)
               logger.trace(Inbound.this.toString() + " considering grabbing more shards given that I have " + numberOfShardsWeActuallyHave + " but could have a max of " + releaseDownToThisMany);

            List<Integer> shardsToTry = null;
            Collection<String> subdirs;
            while(((subdirs = session.getSubdirs(msutils.getShardsDir(), this)).size() < defaultTotalShards) &&
                  (numberOfShardsWeActuallyHave < releaseDownToThisMany))
            {
               if (traceOn)
                  logger.trace(Inbound.this.toString() + " will try to grab more shards.");

               if (shardsToTry == null || shardsToTry.size() == 0)
                  shardsToTry = range(subdirs,defaultTotalShards);
               if (shardsToTry.size() == 0)
               {
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " all other shards are taken.");
                  break;
               }
               int randomIndex = random.nextInt(shardsToTry.size());
               int shardToTry = shardsToTry.remove(randomIndex);
               
               // if we're already considering this shard ...
               if (!doIOwnThisShard(shardToTry,shardNumbersToShards) && 
                     acquireShard(shardToTry, defaultTotalShards, session, clusterId, thisDestination, shardNumbersToShards))
               {
                  if (!destinationsAcquired.contains(shardToTry))
                     destinationsToAdd.add(shardToTry);
                  numberOfShardsWeActuallyHave++;
                  if (traceOn)
                     logger.trace(Inbound.this.toString() + " got a new shard " + shardToTry);

               }
            }
            //==============================================================================
            
            if (destinationsToRemove.size() > 0 || destinationsToAdd.size() > 0)
            {
               String previous = Inbound.this.toString();
               modifyDestinationsAcquired(destinationsToRemove,destinationsToAdd);
               
               if (traceOn)
                  logger.trace(previous + " keyspace notification (" + (destinationsToRemove.size() > 0) + "," + (destinationsToAdd.size() > 0) + ")");
               listener.keyspaceResponsibilityChanged(destinationsToRemove.size() > 0, destinationsToAdd.size() > 0);
            }
            
            if (logger.isTraceEnabled())
               logger.trace(Inbound.this.toString() + " Succesfully reset Inbound Strategy for cluster " + clusterId);

            return true;
         }
         
      };
      //==============================================================================
      
      @Override
      public boolean isInitialized()
      {
         // we are going to assume we're initialized when all of the shards are accounted for.
         // We want to go straight at the cluster info since destinationsAcquired may be out
         // of date in the case where the cluster manager is down.
         try {
            if (nodeDirectory == null)
               return false;
            
            if (!thisDestination.equals(session.getData(nodeDirectory, null)))
               return false;

            if (session.getSubdirs(msutils.getShardsDir(), shardChangeWatcher).size() != defaultTotalShards)
               return false;
            
            // make sure we have all of the nodes we should have
            int numNodes = session.getSubdirs(msutils.getClusterNodesDir(), shardChangeWatcher).size();
            
            return (destinationsAcquired.size() >= (int)Math.floor((double)defaultTotalShards/(double)numNodes)) &&
                  (destinationsAcquired.size() <= (int)Math.ceil((double)defaultTotalShards/(double)numNodes));
         }
         catch (ClusterInfoException e) { return false; }
         catch (RuntimeException re) { logger.debug("",re); return false; }
      }
      
      @Override
      public void stop()
      {
         isRunning.set(false);
      }
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return destinationsAcquired.contains(Math.abs(messageKey.hashCode()%defaultTotalShards));
      }
      
      public int getNumShardsCovered() { return destinationsAcquired.size(); }
      
      private final boolean doIOwnThisShard(Integer shard, Map<Integer,DefaultRouterShardInfo> shardNumbersToShards)
      {
         DefaultRouterShardInfo si = shardNumbersToShards.get(shard);
         if (si == null)
            return false;
         return thisDestination.equals(si.getDestination());
      }
      
      private final int findWorkingNodeCount(ClusterInfoSession session, MicroShardUtils msutils, int minNodeCount, ClusterInfoWatcher nodeDirectoryWatcher) throws ClusterInfoException
      {
         Collection<String> nodeDirs = session.getSubdirs(msutils.getClusterNodesDir(), nodeDirectoryWatcher);
         if (logger.isTraceEnabled())
            logger.trace(toString() + ":Fetching all node's subdirectories:" + nodeDirs);
         
         // We CANNOT only consider node directories that have a Destination because we are not listening for them.
         // The only way to require the destination is to fail here if any destinations are null and retry later
         // in an attempt to wait until whatever node just created this directory gets around to setting the 
         // Destination. For now we will assume if the directory exists, then the node exists.
         int curRegisteredNodesCount = nodeDirs.size();
//         int curRegisteredNodesCount = 0;
//         for (String subdir : nodeDirs)
//         {
//            Destination dest = (Destination)session.getData(msutils.getClusterNodesDir() + "/" + subdir,null);
//            if (dest != null)
//               curRegisteredNodesCount++;
//         }
         return curRegisteredNodesCount < minNodeCount ? minNodeCount : curRegisteredNodesCount;
      }
      
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
      
      public DefaultRouterShardInfo(Destination destination, int shardNum, int totalAddress)
      {
         setDestination(destination);
         setShardIndex(shardNum);
         setTotalAddress(totalAddress);
      }
      
      public DefaultRouterShardInfo() {} // needed for JSON serialization

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
    * This will get the children of one of the main persistent directories (provided in the path parameter). If
    * the directory doesn't exist it will create all of the persistent directories using the passed msutils, but
    * only if the clusterInfo isn't null.
    */
   private static Collection<String> persistentGetMainDirSubdirs(ClusterInfoSession session, MicroShardUtils msutils, 
         String path, ClusterInfoWatcher watcher, DefaultRouterClusterInfo clusterInfo) throws ClusterInfoException
   {
      Collection<String> shardsFromClusterManager;
      try
      {
         shardsFromClusterManager = session.getSubdirs(path, watcher);
      }
      catch (ClusterInfoException.NoNodeException e)
      {
         // clusterInfo == null means that this is a passive call to fillMapFromActiveShards
         // and shouldn't create extraneous directories. If they are not there, there's
         // nothing we can do.
         if (clusterInfo != null)
         {
            msutils.mkAllPersistentAppDirs(session, clusterInfo);
            shardsFromClusterManager = session.getSubdirs(path, watcher);
         }
         else
            shardsFromClusterManager = null;
      }
      return shardsFromClusterManager;
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
      
      // First get the shards that are in transition.
      Collection<String> shardsFromClusterManager = persistentGetMainDirSubdirs(session, msutils, msutils.getShardsDir(),watcher, clusterInfo);

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
         Destination destination, Map<Integer,DefaultRouterShardInfo> shardNumbersToShards) throws ClusterInfoException
   {
      MicroShardUtils utils = new MicroShardUtils(clusterId);
      String shardPath = utils.getShardsDir() + "/" + String.valueOf(shardNum);
      if (clusterHandle.mkdir(shardPath,DirMode.EPHEMERAL) != null)
      {
         DefaultRouterShardInfo dest = (DefaultRouterShardInfo)clusterHandle.getData(shardPath, null);
         if(dest == null)
         {
            dest = new DefaultRouterShardInfo(destination,shardNum,totalAddressNeeded);
            clusterHandle.setData(shardPath, dest);
         }
         if (shardNumbersToShards != null)
            shardNumbersToShards.put(shardNum, dest);
         return true;
      }
      else
         return false;
   }
   
   private final static List<Integer> range(Collection<String> curSubdirs, int max)
   {
      List<Integer> ret = new ArrayList<Integer>(max);
      for (int i = 0; i < max; i++)
      {
         if (!curSubdirs.contains(i))
            ret.add(i);
      }
      return ret;
   }
   
}
