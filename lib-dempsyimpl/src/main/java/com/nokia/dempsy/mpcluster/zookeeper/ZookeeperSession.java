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
import java.util.ArrayList;
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
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpApplication;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

public class ZookeeperSession<T, N> implements MpClusterSession<T, N>
{
   private Logger logger = LoggerFactory.getLogger(ZookeeperSession.class);
   
   protected volatile AtomicReference<ZooKeeper> zkref;
   private volatile boolean isRunning = true;
   private Map<ClusterId, ZookeeperCluster> cachedClusters = new HashMap<ClusterId, ZookeeperCluster>();
   private Map<String, ZookeeperApplication> cachedApps = new HashMap<String, ZookeeperApplication>();
   protected long resetDelay = 500;
   protected String connectString;
   protected int sessionTimeout;
   
   protected ZookeeperSession(String connectString, int sessionTimeout) throws IOException
   {
      this.connectString = connectString;
      this.sessionTimeout = sessionTimeout;
      this.zkref = new AtomicReference<ZooKeeper>();
      ZooKeeper newZk = makeZookeeperInstance(connectString,sessionTimeout);
      if (newZk != null) setNewZookeeper(newZk);
   }
   
   /**
    * This is defined here to be overridden in a test.
    */
   protected ZooKeeper makeZookeeperInstance(String connectString, int sessionTimeout) throws IOException
   {
      return new ZooKeeper(connectString, sessionTimeout, new ZkWatcher());
   }
   
   /**
    * This is defined here to be overridden in a test.
    */
   protected ZookeeperCluster makeZookeeperCluster(ClusterId clusterId) throws MpClusterException
   {
      return new ZookeeperCluster(clusterId);
   }

   @Override
   public MpApplication<T, N> getApplication(String applicationId) throws MpClusterException
   {
      ZookeeperApplication app;
      synchronized(cachedApps)
      {
         app = cachedApps.get(applicationId);
         if (app == null)
         {
            app = new ZookeeperApplication(applicationId);
            initializeApplication(app);
            cachedApps.put(applicationId, app);
         }
      }
      return app;
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
            cluster = makeZookeeperCluster(clusterId);
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
         isRunning = false;
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
   
   private List<String> getChildren(ZookeeperPath path, Watcher watcher) throws MpClusterException
   {
      if (isRunning)
      {
         ZooKeeper cur = zkref.get();
         try
         {
            return cur.getChildren(path.path, watcher);
         }
         catch (KeeperException e) 
         {
            resetZookeeper(cur);
            throw new MpClusterException("Failed to get active slots (" + path + 
                  ") on provided zookeeper instance.",e);
         } 
         catch (InterruptedException e) 
         {
            throw new MpClusterException("Failed to get active slots (" + path + 
                  ") on provided zookeeper instance.",e);
         }
      }
      throw new MpClusterException("getChildren called on stopped MpCluster (" + path + 
            ") on provided zookeeper instance.");
   }
   
   private synchronized void setNewZookeeper(ZooKeeper newZk)
   {
      if (logger.isTraceEnabled())
         logger.trace("reestablished connection to " + connectString);
      
      if (isRunning)
      {
         ZooKeeper last = zkref.getAndSet(newZk);
         if (last != null)
         {
            try { last.close(); } catch (Throwable th) {}
         }
      }
      else
      {
         // in this case with zk == null we're shutting down.
         try { newZk.close(); } catch (Throwable th) {}
      }
   }
   
   protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   protected volatile boolean beingReset = false;
   
   private synchronized void resetZookeeper(ZooKeeper failedInstance)
   {
      AtomicReference<ZooKeeper> tmpZkRef = zkref;
      // if we're not shutting down (which would be indicated by tmpZkRef == null
      //   and if the failedInstance we're trying to reset is the current one, indicated by tmpZkRef.get() == failedInstance
      //   and if we're not already working on beingReset
      if (tmpZkRef != null && tmpZkRef.get() == failedInstance && !beingReset)
      {
         beingReset = true;
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
                  if (newZk == null && isRunning)
                     // reschedule me.
                     scheduler.schedule(this, resetDelay, TimeUnit.MILLISECONDS);
               }

               // this is true if the reset worked and we're not in the process
               // of shutting down.
               if (newZk != null && isRunning)
               {
                  // we want the setNewZookeeper and the clearing of the
                  // beingReset flag to be atomic so future failures that result
                  // in calls to resetZookeeper will either:
                  //   1) be skipped because they are for an older ZooKeeper instance.
                  //   2) be executed because they are for this new ZooKeeper instance.
                  // what we dont want is the possibility that the reset will be skipped
                  // even though the reset is called for this new ZooKeeper, but we haven't cleared
                  // the beingReset flag yet.
                  synchronized(ZookeeperSession.this)
                  {
                     setNewZookeeper(newZk);
                     beingReset = false;
                  }
                  
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
   
   private boolean mkdir(ZooKeeper cur, String path, Watcher watcher, CreateMode mode) 
   {
      try 
      {
         Stat s = watcher == null ? cur.exists(path,false) : cur.exists(path, watcher);
         if (s == null)
         {
            try
            {
               cur.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
            }
            // this is actually ok. It means we lost the race.
            catch (KeeperException.NodeExistsException nee) { }
            if (watcher == null ? (cur.exists(path, false) == null) : (cur.exists(path, watcher) == null))
            {
               logger.error("Could neither create nor get the node for " + path);
               resetZookeeper(cur);
            }
         }
         return true;
      }
      catch (KeeperException e) 
      {
         logger.warn("Failed to create the root node (" + path + ") on provided zookeeper instance.",e);
         resetZookeeper(cur);
      } 
      catch (InterruptedException e) 
      {
         logger.warn("Attempt to initialize the zookeeper client for (" + path + ") was interrupted.",e);
         resetZookeeper(cur);
      }
      return false;
   }
   
   private void initializeApplication(ZookeeperApplication application)
   {
      ZooKeeper cur = null;
      if (isRunning)
         cur = zkref.get();

      if (cur != null)
      {
         // set the root node
         mkdir(cur,application.applicationPath.path,application,CreateMode.PERSISTENT);
      }
   }

   private void initializeCluster(ZookeeperCluster cluster, boolean forceWatcherCall)
   {
      // if we're here lets reset the slots
      synchronized(cluster)
      {
         cluster.allSlots = null;
      }

      ZooKeeper cur = null;
      if (isRunning)
         cur = zkref.get();

      if (cur != null)
      {
         // set the root node
         if (mkdir(cur,cluster.appPath.path,null,CreateMode.PERSISTENT))
            if (mkdir(cur,cluster.clusterPath.path, cluster,CreateMode.PERSISTENT))
               if (forceWatcherCall)
                  cluster.process(null);
      }
   }
   
   private class WatcherManager implements Watcher
   {
      private CopyOnWriteArraySet<MpClusterWatcher> watchers = new CopyOnWriteArraySet<MpClusterWatcher>();
      private Object processLock = new Object();
      private String idForLogging;
      
      private WatcherManager(String idForLogging) { this.idForLogging = idForLogging; }
      
      public void addWatcher(MpClusterWatcher watch)
      {
         // set semantics adds it only if it's not there already
         watchers.add(watch); // to avoid a potential race condition, we clear the allSlots
         clearState(null);
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
      
      protected void clearState(WatchedEvent event) {}
      
      @Override
      public void process(WatchedEvent event)
      {
         // event = null means it was called explicitly
         if (logger.isDebugEnabled() && event != null)
            logger.debug("CALLBACK:MpContainerCluster for " + idForLogging + " Event:" + event);

         boolean kickOffProcess = true;
         if (event != null)
         {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged)
               clearState(event);

            // when we're not connected we want to reset
            if (event.getState() != KeeperState.SyncConnected)
            {
               kickOffProcess = false; // no reason to execute process if we're going to reset zookeeper.
               
               clearState(event);

               if (isRunning)
                  resetZookeeper(zkref.get());
            }
         }

         if (kickOffProcess)
         {
            synchronized(processLock)
            {
               for(MpClusterWatcher watch: watchers)
                  watch.process();
            }
         }
      }
   }
   
   public class ZookeeperApplication extends WatcherManager implements MpApplication<T,N>
   {
      ZookeeperPath applicationPath;
      String applicationId;
      
      private ZookeeperApplication(String applicationName)
      {
         super(applicationName);
         this.applicationId = applicationName;
         applicationPath = new ZookeeperPath(applicationId);
      }
      
      @Override
      public Collection<MpCluster<T, N>> getActiveClusters() throws MpClusterException
      {
         List<String> clusters = getChildren(applicationPath, this);
         List<MpCluster<T,N>> ret = new ArrayList<MpCluster<T,N>>(clusters.size());
         for (String cluster : clusters)
            ret.add(getCluster(new ClusterId(applicationId,cluster)));
         return ret;
      }
   }

   public class ZookeeperCluster extends WatcherManager implements MpCluster<T, N>
   {
      private ClusterId clusterId;
      private Map<String,MpClusterSlot<N>> allSlots = null;
      private ZookeeperPath clusterPath;
      private ZookeeperPath appPath;
      
      protected ZookeeperCluster(ClusterId clusterId) throws MpClusterException
      {
         super(clusterId.toString());
         this.clusterId = clusterId;
         clusterPath = new ZookeeperPath(clusterId,false);
         appPath = new ZookeeperPath(clusterId,true);
      }
      
      @Override
      public synchronized Collection<MpClusterSlot<N>> getActiveSlots() throws MpClusterException
      {
         if (allSlots == null)
         {
            List<String> list = getChildren(clusterPath,this);

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

      @Override
      protected void clearState(WatchedEvent event)
      {
         synchronized(this)
         {
            if (logger.isTraceEnabled() && event != null)
               logger.debug("Clearing all slots because of " + event);
            allSlots = null;
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

      private class ZkClusterSlot<TS> implements MpClusterSlot<TS>
      {
         private ZookeeperPath slotPath;
         private String slotName;
         private Serializer<TS> serializer;
         
         public ZkClusterSlot(String slotName)
         {
            this.slotName = slotName;
            this.slotPath = new ZookeeperPath(clusterPath, slotName);
            serializer = new JSONSerializer<TS>();
         }
         
         @Override
         public String getSlotName() { return slotName; }
         
         @Override
         public TS getSlotInformation() throws MpClusterException
         {
            byte[] slot = null;
            try
            {
               slot = zk.get().getData(slotPath.path, true, null);
            }
            catch(Exception e)
            {
               throw new MpClusterException("Error reading slot data for path "+slotPath.path, e);
            }
            try
            {
               return (slot==null||slot.length==0)?null:serializer.deserialize(slot);
            }
            catch(Exception e)
            {
               throw new MpClusterException("Error deserializing slot data "+slot+" for path "+ slotPath.path, e);
            }
         }

         public void setSlotInformation(TS info) throws MpClusterException
         {
            byte[] slotData = null;
            try
            {
               slotData = serializer.serialize(info);
            }
            catch(Exception e)
            {
               throw new MpClusterException("Error serializing data "+
                     info + " to cluster path "+ slotPath.path, e);
            }
            try
            {
               zk.get().setData(slotPath.path, slotData, -1);
            }
            catch(Exception e)
            {
               throw new MpClusterException("Error writing data "+
                     info + " to cluster path "+slotPath.path, e);
            }
         }

         private boolean join() throws MpClusterException
         {
            if (isRunning)
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
            
            throw new MpClusterException("join called on stopped MpClusterSlot (" + slotPath + 
                  ") on provided zookeeper instance.");

         }

         @Override
         public synchronized void leave() throws MpClusterException
         {
            if (isRunning)
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
            else
               throw new MpClusterException("leave called on stopped MpClusterSlot (" + slotPath + 
                     ") on provided zookeeper instance.");
         }
         
         public String toString() { return slotPath.toString(); }
         
         @SuppressWarnings("hiding")
         private class JSONSerializer<TS> implements Serializer<TS>
         {
            ObjectMapper objectMapper;
            
            public JSONSerializer()
            {
               objectMapper = new ObjectMapper();
               objectMapper.enableDefaultTyping();
               objectMapper.configure(SerializationConfig.Feature.WRITE_EMPTY_JSON_ARRAYS, true);
               objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
               objectMapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, true);
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public TS deserialize(byte[] data) throws SerializationException
            {
               ArrayList<TS> info = null;
               if(data != null)
               {
                  String jsonData = new String(data);
                  try
                  {
                     info = objectMapper.readValue(jsonData, ArrayList.class);
                  }
                  catch(Exception e)
                  {
                     throw new SerializationException("Error occured while deserializing data "+jsonData, e);
                  }
               }
               return (info != null && info.size()>0)?info.get(0):null;
            }
            
            @Override
            public byte[] serialize(TS data) throws SerializationException 
            {
               String jsonData = null;
               if(data != null)
               {
                  ArrayList<TS> arr = new ArrayList<TS>();
                  arr.add(data);
                  try
                  {
                     jsonData = objectMapper.writeValueAsString(arr);
                  }
                  catch(Exception e)
                  {
                     throw new SerializationException("Error occured during serializing class " +
                           SafeString.valueOfClass(data) + " with information "+SafeString.valueOf(data), e);
                  }
               }
               return (jsonData != null)?jsonData.getBytes():null;
            }
         }
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
      
      public ZookeeperPath(String applicationName)
      {
         path = root + applicationName;
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
      if (isRunning)
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
      return null;
   }
   
   private void setInfoToPath(ZookeeperPath path, Object info) throws MpClusterException
   {
      if (isRunning)
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

}
