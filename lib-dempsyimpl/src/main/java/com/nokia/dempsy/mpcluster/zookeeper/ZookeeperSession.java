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

package com.nokia.dempsy.mpcluster.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

public class ZookeeperSession<T, N> implements MpClusterSession<T, N>
{
   private Logger logger = LoggerFactory.getLogger(ZookeeperSession.class);
   
   protected AtomicReference<ZooKeeper> zkref;
   private Map<ClusterId, ZookeeperCluster> cachedClusters = new HashMap<ClusterId, ZookeeperCluster>();
   protected long resetDelay = 500;
   protected String connectString;
   protected int sessionTimeout;
   
   protected ZookeeperSession(String connectString, int sessionTimeout) throws IOException
   {
      this.connectString = connectString;
      this.sessionTimeout = sessionTimeout;
      this.zkref = new AtomicReference<ZooKeeper>();
      this.zkref.set(makeZookeeperInstance(connectString,sessionTimeout));
   }
   
   /**
    * This is defined here to be overridden in a test.
    */
   protected ZooKeeper makeZookeeperInstance(String connectString, int sessionTimeout) throws IOException
   {
      return new ZooKeeper(connectString, sessionTimeout, new ZkWatcher());
   }

   
   @Override
   public MpCluster<T, N> getCluster(ClusterId clusterId) throws MpClusterException
   {
      ZookeeperCluster cluster;
      synchronized(cachedClusters)
      {
         cluster = cachedClusters.get(clusterId);
         if (cluster == null)
         {
            // make sure the paths are set up
            cluster = new ZookeeperCluster(clusterId);
            initializeCluster(cluster,false);
            cachedClusters.put(clusterId, cluster);
         }
      }
      
      return cluster;
   }
   
   @Override
   public void stop()
   {
      AtomicReference<ZooKeeper> curZk;
      synchronized(this)
      {
         curZk = zkref;
         zkref = null; // this blows up any more usage
      }
      Set<ZookeeperCluster> tmp = new HashSet<ZookeeperCluster>();
      synchronized(cachedClusters)
      {
         tmp.addAll(cachedClusters.values());
      }
      
      // stop all of the ZookeeperClusters first
      for (ZookeeperCluster cluster : tmp)
         cluster.stop();
      try { curZk.get().close(); } catch (Throwable th) { /* let it go otherwise */ }
   }
   
   private List<String> getChildren(ZookeeperCluster cluster) throws MpClusterException
   {
      ZooKeeper cur = zkref.get();
      try
      {
         return cur.getChildren(cluster.clusterPath.path, cluster);
      }
      catch (KeeperException e) 
      {
         resetZookeeper(cur);
         throw new MpClusterException("Failed to get active slots (" + cluster.clusterPath + 
               ") on provided zookeeper instance.",e);
      } 
      catch (InterruptedException e) 
      {
         throw new MpClusterException("Failed to get active slots (" + cluster.clusterPath + 
               ") on provided zookeeper instance.",e);
      }
   }
   
   protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   protected ZooKeeper beingReset = null;
   
   private synchronized void setNewZookeeper(ZooKeeper newZk)
   {
      if (logger.isTraceEnabled())
         logger.trace("reestablished connection to " + connectString);
      
      if (zkref != null)
      {
         ZooKeeper last = zkref.getAndSet(newZk);
         if (last != null)
         {
            try { last.close(); } catch (Throwable th) {}
         }
         beingReset = null;
      }
      else
      {
         // in this case with zk == null we're shutting down.
         try { newZk.close(); } catch (Throwable th) {}
      }
   }
   
   private synchronized void resetZookeeper(ZooKeeper failedInstance)
   {
      if (beingReset != failedInstance)
      {
         beingReset = failedInstance;

         scheduler.schedule(new Runnable()
         {
            @Override
            public void run()
            {
               ZooKeeper newZk = null;
               try
               {
                  newZk = makeZookeeperInstance(connectString, sessionTimeout);
               }
               catch (IOException e)
               {
                  logger.warn("Failed to reset the ZooKeeper connection to " + connectString);
                  newZk = null;
               }
               finally
               {
                  if (newZk == null && zkref != null) // if zk is null then we stopped so no point in continuing.
                     // reschedule me.
                     scheduler.schedule(this, resetDelay, TimeUnit.MILLISECONDS);
               }

               // this is true if the reset worked and we're not in the process
               // of shutting down.
               if (newZk != null && zkref != null)
               {
                  setNewZookeeper(newZk);

                  // now reset the watchers
                  Set<ZookeeperCluster> clustersToReset = new HashSet<ZookeeperCluster>();
                  synchronized(cachedClusters)
                  {
                     clustersToReset.addAll(cachedClusters.values());
                  }

                  for (ZookeeperCluster cluster : clustersToReset)
                     initializeCluster(cluster,true);
               }
               else if (newZk != null)
               {
                  // in this case with zk == null we're shutting down.
                  try { newZk.close(); } catch (Throwable th) {}
               }
            }
         }, resetDelay, TimeUnit.MILLISECONDS);
      }
   }

   private void initializeCluster(ZookeeperCluster cluster, boolean forceWatcherCall)
   {
      // if we're here lets reset the slots
      synchronized(cluster)
      {
         cluster.allSlots = null;
      }
      
      // set the root node
      ZooKeeper cur = zkref.get();
      try 
      {
         Stat s = cur.exists(cluster.appPath.path,false);
         if (s == null)
         {
            try
            {
               cur.create(cluster.appPath.path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // this is actually ok. It means we lost the race.
            catch (KeeperException.NodeExistsException nee) { }
         }

         s = cur.exists(cluster.clusterPath.path, cluster);
         if (s == null) 
         {
            try
            {
               cur.create(cluster.clusterPath.path, new byte[0], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            // this is actually ok. It means we lost the race.
            catch (KeeperException.NodeExistsException nee) { }
            if (cur.exists(cluster.clusterPath.path, cluster) == null)
            {
               logger.error("Could neither create nor get the cluster node for " + cluster.clusterPath.path);
               resetZookeeper(cur);
            }
         }
         else if (forceWatcherCall)
         {
            cluster.process(null);
         }
      }
      catch (KeeperException e) 
      {
         logger.error("Failed to create the root node (" + cluster.clusterPath + ") on provided zookeeper instance.",e);
         resetZookeeper(cur);
      } 
      catch (InterruptedException e) 
      {
         logger.warn("Attempt to initialize the zookeeper client for (" + cluster.clusterPath + ") was interrupted.",e);
         resetZookeeper(cur);
      }
      catch (NullPointerException npe)
      {
         // this means zk has been set to null which means we're stopping so just allow it to stop
      }

   }

   public class ZookeeperCluster implements MpCluster<T, N>, Watcher
   {
      private ClusterId clusterId;
      private Map<String,MpClusterSlot<N>> allSlots = null;
      private ZookeeperPath clusterPath;
      private ZookeeperPath appPath;
      
      private CopyOnWriteArraySet<MpClusterWatcher> watchers = new CopyOnWriteArraySet<MpClusterWatcher>();
      
      private Object processLock = new Object();
      
      public ZookeeperCluster(ClusterId clusterId) throws MpClusterException
      {
         this.clusterId = clusterId;
         clusterPath = new ZookeeperPath(clusterId,false);
         appPath = new ZookeeperPath(clusterId,true);
      }
      
      @Override
      public synchronized Collection<MpClusterSlot<N>> getActiveSlots() throws MpClusterException
      {
         if (allSlots == null)
         {
            List<String> list = getChildren(this);

            allSlots = new HashMap<String, MpClusterSlot<N>>();
            for (String nodeString : list)
               allSlots.put(nodeString, new ZkClusterSlot<N>(nodeString));
         }
         return allSlots.values();
      }
      
      @Override
      public ClusterId getClusterId() { return clusterId; }
      
      /**
       * Joins the cluster slot with given nodeName
       * 
       * @return {@link MpClusterSlot} the newly created cluster slot if successful, else returns null.
       * 
       * @exception MpClusterException - due to connectivity issues, interrupts or illegal arguments. 
       */
      @Override
      public synchronized MpClusterSlot<N> join(String nodeName) throws MpClusterException
      {
         ZkClusterSlot<N> ret  = new ZkClusterSlot<N>(nodeName);
         return ret.join() ? ret : null;
      }
      
      private void clearAllSlots(WatchedEvent event)
      {
         synchronized(this)
         {
            if (logger.isTraceEnabled() && event != null)
               logger.debug("Clearing all slots because of " + event);
            allSlots = null;
         }
      }

      @Override
      public void process(WatchedEvent event)
      {
         // even = null means it was called explcitly by the initializeCluster
         if (logger.isDebugEnabled() && event != null)
            logger.debug("CALLBACK:MpContainerCluster for " + clusterId.toString() + " Event:" + event);
         
         if (event != null)
         {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged)
               clearAllSlots(event);

            if (event.getState() != KeeperState.SyncConnected)
            {
               clearAllSlots(event);

               if (zkref != null)
                  resetZookeeper(zkref.get());
            }
         }
         
         synchronized(processLock)
         {
            for(MpClusterWatcher watch: watchers)
               watch.process();
         }
      }
      
      /**
       * This method makes sure there are no watchers running a process and
       * clears the list of watchers.
       */
      protected void stop()
      {
         watchers.clear();
         
         synchronized(processLock)
         {
            // this just holds up if process is currently running ...
            // if process isn't running then the above clear should 
            //   prevent and watcher.process calls from ever being made.
         }
      }
      
      @Override
      public void setClusterData(T data) throws MpClusterException 
      {
         setInfoToPath(clusterPath,data);
      }
      
      @SuppressWarnings("unchecked")
      @Override
      public T getClusterData() throws MpClusterException
      {
         return (T)readInfoFromPath(clusterPath);
      }

      @Override
      public void addWatcher(MpClusterWatcher watch)
      {
         // set semantics adds it only if it's not there already
         watchers.add(watch); // to avoid a potential race condition, we clear the allSlots
         clearAllSlots(null);
      }
      
      private class ZkClusterSlot<TS> implements MpClusterSlot<TS>
      {
         private ZookeeperPath slotPath;
         private String slotName;
         
         public ZkClusterSlot(String slotName)
         {
            this.slotName = slotName;
            this.slotPath = new ZookeeperPath(clusterPath, slotName);
         }
         
         @Override
         public String getSlotName() { return slotName; }
         
         @SuppressWarnings("unchecked")
         @Override
         public TS getSlotInformation() throws MpClusterException
         {
            return (TS)readInfoFromPath(slotPath);
         }

         public void setSlotInformation(TS info) throws MpClusterException
         {
            setInfoToPath(slotPath,info);
         }

         private boolean join() throws MpClusterException
         {
            ZooKeeper cur = zkref.get();
            try
            {
               cur.create(slotPath.path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
               return true;
            }
            catch(KeeperException.NodeExistsException e)
            {
               if(logger.isDebugEnabled())
                  logger.debug("Failed to join the cluster " + clusterId + 
                        ". Couldn't create the node within zookeeper using \"" + slotPath + "\"");
               return false;
            }
            catch(KeeperException e)
            {
               resetZookeeper(cur);
               throw new MpClusterException("Zookeeper failed while trying to join the cluster " + clusterId + 
                     ". Couldn't create the node within zookeeper using \"" + slotPath + "\"",e);
            }
            catch(InterruptedException e)
            {
               resetZookeeper(cur);
               throw new MpClusterException("Interrupted while trying to join the cluster " + clusterId + 
                     ". Couldn't create the node within zookeeper using \"" + slotPath + "\"",e);
            }

         }

         @Override
         public synchronized void leave() throws MpClusterException
         {
            try
            {
               zkref.get().delete(slotPath.path,-1);
            }
            catch(KeeperException e)
            {
               throw new MpClusterException("Failed to leave the cluster " + clusterId.getApplicationName() + 
                     ". Couldn't delete the node within zookeeper using \"" + slotPath + "\"",e);
            }
            catch(InterruptedException e)
            {
               throw new MpClusterException("Interrupted while trying to leave the cluster " + clusterId.getApplicationName() + 
                     ". Couldn't delete the node within zookeeper using \"" + slotPath + "\"",e);
            }
         }
         
         public String toString() { return slotPath.toString(); }
      }
      
   }

   /**
    * Helper class for calculating the path within zookeeper given the 
    */
   private static class ZookeeperPath
   {
      public static String root = "/";
      public String path;
      
      public ZookeeperPath(ClusterId clusterId, boolean appPathOnly)
      {
         path = root + clusterId.getApplicationName() +
               (!appPathOnly ? ("/" + clusterId.getMpClusterName()) : "");
      }
      
      public ZookeeperPath(ZookeeperPath root, String path) { this.path = root.path + "/" + path; }
      
      public String toString() { return path; }
   }
   
   /*
    * Protected access is for testing.
    */
   protected class ZkWatcher implements Watcher
   {
      @Override
      public void process(WatchedEvent event)
      {
         if (logger.isTraceEnabled())
            logger.trace("CALLBACK:Main Watcher:" + event);
      }
   }
   
   private Object readInfoFromPath(ZookeeperPath path) throws MpClusterException
   {
      ObjectInputStream is = null;
      try
      {
         byte[] ret = zkref.get().getData(path.path, true, null);
         
         if (ret != null && ret.length > 0)
         {
            is = new ObjectInputStream(new ByteArrayInputStream(ret));
            return is.readObject();
         }
         return null;
      }
      // this is an indication that the node has disappeared since we retrieved 
      // this MpContainerClusterNode
      catch (KeeperException.NoNodeException e) { return null; }
      catch (RuntimeException e) { throw e; } 
      catch (Exception e) 
      {
         throw new MpClusterException("Failed to get node information for (" + path + ").",e);
      }
      finally
      {
         IOUtils.closeQuietly(is);
      }
   }
   
   private void setInfoToPath(ZookeeperPath path, Object info) throws MpClusterException
   {
      ObjectOutputStream os = null;
      try
      {
         byte[] buf = null;
         if (info != null)
         {
            // Serialize to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);
            os.writeObject(info);
            os.close(); // flush

            // Get the bytes of the serialized object
            buf = bos.toByteArray();
         }
         
         zkref.get().setData(path.path, buf, -1);
      }
      catch (RuntimeException e) { throw e;} 
      catch (Exception e) 
      {
         throw new MpClusterException("Failed to get node information for (" + path + ").",e);
      }
      finally
      {
         IOUtils.closeQuietly(os);
      }
   }


}
