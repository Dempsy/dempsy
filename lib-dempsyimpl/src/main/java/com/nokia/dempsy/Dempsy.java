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

package com.nokia.dempsy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.ContainerException;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.StatsCollectorFactory;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSessionFactory;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;
import com.nokia.dempsy.router.ClusterInformation;
import com.nokia.dempsy.router.CurrentClusterCheck;
import com.nokia.dempsy.router.Router;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.router.SlotInformation;
import com.nokia.dempsy.serialization.Serializer;

/**
 * This class is the master orchestrator of the setup of a VM running the application.
 * It is intended to have the ApplicationDefinitions autowired in so it picks up
 * each one.
 */
public class Dempsy
{
   static Logger logger = LoggerFactory.getLogger(Dempsy.class);
   
   /**
    * There is an instance of an application for every ApplicationDefinition
    * autowired into the Dempsy instance. In most real cases this will be only
    * one.
    */
   public class Application
   {
      /**
       * Currently a "Dempsy.Application.Cluster" has no utility since it only has a single Node.
       * This may change
       */
      public class Cluster
      {
         /**
          * A Node is essentially an {@link MpContainer} with all of the attending infrastructure.
          * Currently a Node is instantiated within the Dempsy orchestrator as a one to one with the
          * {@link Cluster}.
          */
         public class Node implements MpClusterWatcher<ClusterInformation,SlotInformation>
         {
            protected ClusterDefinition clusterDefinition;
            
            private Router router = null;
            private MpContainer container = null;
            private RoutingStrategy.Inbound strategyInbound = null;
            List<Class<?>> acceptedMessageClasses = null;
            Receiver receiver = null;
            StatsCollector statsCollector = null;
            
            private Node(ClusterDefinition clusterDefinition) { this.clusterDefinition = clusterDefinition; }
            
            @SuppressWarnings("unchecked")
            private void start() throws DempsyException
            {
               try
               {
                  MpClusterSession<ClusterInformation, SlotInformation> clusterSession = clusterSessionFactory.createSession();
                  ClusterId currentClusterId = clusterDefinition.getClusterId();
                  router = new Router(clusterDefinition.getParentApplicationDefinition());
                  router.setCurrentCluster(currentClusterId);
                  router.setClusterSession(clusterSession);
                  router.setDefaultSenderFactory(transport.createOutbound());
                  
                  container = new MpContainer(currentClusterId);
                  container.setDispatcher(router);
                  Object messageProcessorPrototype = clusterDefinition.getMessageProcessorPrototype();
                  if (messageProcessorPrototype != null)
                  {
                    preInitializePrototype(messageProcessorPrototype);
                     container.setPrototype(messageProcessorPrototype);
                  }
                  acceptedMessageClasses = getAcceptedMessages(clusterDefinition);
                  
                  Serializer<Object> serializer = (Serializer<Object>)clusterDefinition.getSerializer();
                  if (serializer != null)
                     container.setSerializer(serializer);
                  
                  // there is only a reciever if we have an Mp (that is, we aren't an adaptor) and start accepting messages 
                  if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0)
                  {
                     receiver = transport.createInbound();
                     receiver.setListener(container);
                  }

                  StatsCollectorFactory statsFactory = (StatsCollectorFactory)clusterDefinition.getStatsCollectorFactory();
                  if (statsFactory != null)
                  {
                     statsCollector = statsFactory.createStatsCollector(currentClusterId, 
                           receiver != null ? receiver.getDestination() : null);
                     router.setStatsCollector(statsCollector);
                     container.setStatCollector(statsCollector);
                  }
                  
                  if (clusterDefinition.getOutputSchedule()!=null)
                     container.setOutPutPass(clusterDefinition.getOutputSchedule().getInterval(), clusterDefinition.getOutputSchedule().getTimeUnit());

                  RoutingStrategy strategy = (RoutingStrategy)clusterDefinition.getRoutingStrategy();
                  
                  // there is only an inbound strategy if we have an Mp (that is, we aren't an adaptor) and
                  // we actually accept messages
                  if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0)
                     strategyInbound = strategy.createInbound();
                  
                  MpCluster<ClusterInformation, SlotInformation> currentClusterHandle = clusterSession.getCluster(currentClusterId);
                  
                  // this can fail because of down cluster manager server ... but it should eventually recover.
                  try
                  {
                     if (strategyInbound != null && receiver != null)
                        strategyInbound.resetCluster(currentClusterHandle, acceptedMessageClasses, receiver.getDestination());
                     router.initialize();
                  }
                  catch (MpClusterException e)
                  {
                     logger.warn("Strategy failed to initialize. Continuing anyway. The cluster manager issue will be resolved automatically.",e);
                  }
                  
                  Adaptor adaptor = clusterDefinition.isRouteAdaptorType() ? clusterDefinition.getAdaptor() : null;
                  if (adaptor != null)
                     adaptor.setDispatcher(router);
                  
                  final KeyStore<?> keyStore = clusterDefinition.getKeyStore();
                  if(keyStore != null)
                  {
                     Thread t = new Thread(new Runnable()
                     {
                        @Override
                        public void run()
                        {
                           try{
                              statsCollector.preInstantiationStarted();
                              Iterable<?> iterable = keyStore.getAllPossibleKeys();
                              for(Object key: iterable)
                              {
                                 try
                                 {
                                    if(strategyInbound.doesMessageKeyBelongToCluster(key))
                                    {
                                          container.getInstanceForKey(key);
                                    }
                                 }
                                 catch(ContainerException e)
                                 {
                                    logger.error("Failed to instantiate MP for Key "+key, e);
                                 }
                              }
                           }
                           finally
                           {
                              statsCollector.preInstantiationCompleted();
                           }
                        }
                     }, "Pre-Instantation Thread");
                     t.start();
                  }
                  
                  // now we want to set the Node as the watcher.
                  if (strategyInbound != null && receiver != null)
                  {
                     currentClusterHandle.addWatcher(this);
                  
                     // and reset just in case something happened
                     try
                     {
                        strategyInbound.resetCluster(currentClusterHandle, acceptedMessageClasses, receiver.getDestination());
                     }
                     catch (MpClusterException e)
                     {
                        logger.warn("Strategy failed to initialize. Continuing anyway. The cluster manager issue will be resolved automatically.",e);
                     }
                  }
               }
               catch(RuntimeException e) { throw e; }
               catch(Exception e) { throw new DempsyException(e); }
            }
            
            public StatsCollector getStatsCollector() { return statsCollector; }
            
            public MpContainer getMpContainer() { return container; }

            @Override
            public void process(MpCluster<ClusterInformation, SlotInformation> cluster)
            {
               try
               {
                  if (strategyInbound != null)
                     strategyInbound.resetCluster(cluster, acceptedMessageClasses, receiver.getDestination());
               }
               // TODO: fix these catches... .need to take note of a failure for a later retry
               // using a scheduled task.
               catch(RuntimeException e) { throw e; }
               catch(Exception e) { throw new RuntimeException(e); }
            }
            
            public void stop()
            {
               if (receiver != null) 
               {
                  try { receiver.stop(); receiver = null; }
                  catch (Throwable th)
                  {
                     logger.error("Error stoping the reciever " + SafeString.objectDescription(receiver) + 
                           " for " + SafeString.valueOf(clusterDefinition) + " due to the following exception:",th);
                  }
               }

               // shut the container down prior to the router.
               if (container != null)
                  try { container.shutdown(); container = null; } catch (Throwable th) { logger.error("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
               
               // the cluster session is stopped by the router.
               if (router != null)
                  try { router.stop(); router = null; } catch (Throwable th) { logger.error("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
               
               if (statsCollector != null)
                  try { statsCollector.stop(); statsCollector = null;} catch (Throwable th) { logger.error("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }

            }
            
            /**
             * Run any methods annotated PreInitilze on the MessageProcessor prototype
             * @param prototype reference to MessageProcessor prototype
             */
            private void preInitializePrototype(Object prototype) {
               // TODO, should this return a boolean and stop the cluster from starting?
               
               for (Method method: prototype.getClass().getMethods()) {
                  if (method.isAnnotationPresent(com.nokia.dempsy.annotations.Start.class)) {
                     try {
                        method.invoke(prototype);
                        break;  // Only allow one such method, which is checked during validation
                     }catch(Exception e) {
                        logger.error(MarkerFactory.getMarker("FATAL"), "can't run MP initializer " + method.getName(), e);
                     }
                  }
               }
            }
            
         } // end Node definition
         
         private List<Node> nodes = new ArrayList<Node>(1);
         private ClusterDefinition clusterDefinition;
         
         private Cluster(ClusterDefinition clusterDefinition)
         {
            this.clusterDefinition = clusterDefinition;
            allClusters.put(clusterDefinition.getClusterId(),this);
         }
         
         private void start() throws DempsyException
         {
            nodes.clear();
            Node node = new Node(clusterDefinition);
            nodes.add(node);
            node.start();
         }
         
         private void stop()
         {
            for(Node node : nodes)
               node.stop();
         }
         
         public List<Node> getNodes() { return nodes; }
         
         /**
          * This is only public for testing - DO NOT CALL!
          * @throws DempsyException
          */
         public void instantiateAndStartAnotherNodeForTesting() throws DempsyException
         {
            Node node = new Node(clusterDefinition);
            nodes.add(node);
            node.start();
         }
         
      } // end Cluster Definition
      
      private ApplicationDefinition applicationDefinition;
      private List<Cluster> appClusters = new ArrayList<Cluster>();
      private List<AdaptorThread> adaptorThreads = new ArrayList<AdaptorThread>();
      
      public Application(ApplicationDefinition applicationDefinition)
      {
         this.applicationDefinition = applicationDefinition;  
      }
      
      private DempsyException failedStart = null;
      public class ClusterStart implements Runnable
      {
         private Cluster cluster;
         private ClusterStart(Cluster cluster) { this.cluster = cluster; }
         public void run() { try { cluster.start(); } catch(DempsyException e) { failedStart = e; } } 
      }
      
      /**
       * 
       * @return boolean - true if starts the cluster, else false
       * @throws DempsyException
       */
      public boolean start() throws DempsyException
      {
         failedStart = null;
         boolean clusterStarted = false;
         
         for (ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions())
         {
            if(clusterCheck.isThisNodePartOfCluster(clusterDef.getClusterId()))
            {
               Cluster cluster = new Cluster(clusterDef);
               appClusters.add(cluster);
            }
         }
         
         List<Thread> toJoin = new ArrayList<Thread>(appClusters.size());
         
         for (Cluster cluster : appClusters)
         {
//            if (multiThreadedStart)
//            {
//               Thread t = new Thread(new ClusterStart(cluster),"Starting cluster:" + cluster.clusterDefinition);
//               toJoin.add(t);
//               t.start();
//               clusterStarted = true;
//            }
//            else 
//            {
               cluster.start();
               clusterStarted = true;
//            }
         }
         
         for (Thread t : toJoin)
         {
            try { t.join(); } catch(InterruptedException e) { /* just continue */ }
         }
         
         if (failedStart != null)
            throw failedStart;
         
         for (Cluster cluster : appClusters)
         {
            if (clusterCheck.isThisNodePartOfCluster(cluster.clusterDefinition.getClusterId()))
            {
               Adaptor adaptor = cluster.clusterDefinition.getAdaptor();
               if (adaptor != null)
               {
                  AdaptorThread adaptorRunnable = new AdaptorThread(adaptor);
                  Thread thread = new Thread(adaptorRunnable , "Adaptor - " + SafeString.objectDescription(adaptor) );
                  adaptorThreads.add(adaptorRunnable);
                  if (cluster.clusterDefinition.isAdaptorDaemon())
                     thread.setDaemon(true);
                  thread.start();
                  clusterStarted = true;
               }
            }
         }
         return clusterStarted;
      }
      
      public void stop()
      {
         // first stop all of the adaptor threads
         for(AdaptorThread adaptorThread : adaptorThreads)
            adaptorThread.stop();

         // stop all of the non-adaptor clusters.
         for(Cluster cluster : appClusters)
            cluster.stop();
      }
      
   } // end Application definition
   
   private static class AdaptorThread implements Runnable
   {
      private Adaptor adaptor = null;
      private Thread thread = null;
      
      public AdaptorThread(Adaptor adaptor) { this.adaptor = adaptor; }
      
      @Override
      public void run()
      {
         thread = Thread.currentThread();
         logger.info("Starting adaptor thread for " + SafeString.objectDescription(adaptor));
         try { adaptor.start(); }
         catch (Throwable th) { logger.warn("Adaptor " + SafeString.objectDescription(adaptor) + " threw an unexpected exception.", th); }
         finally { logger.info("Adaptor thread for " + SafeString.objectDescription(adaptor) + " is shutting down"); }

      }
      
      /**
       * This is a helper method that should stop the running thread by initiating
       * a stop on the adaptor, then interrupting the thread in case it's in a 
       * blocking call somewhere.
       */
      public void stop()
      {
         try { if (adaptor != null) adaptor.stop(); } catch (Throwable th) { logger.error("Problem trying to stop the Adaptor " + SafeString.objectDescription(adaptor), th); }
         if (thread != null) thread.interrupt();
      }
   }

   private List<ApplicationDefinition> applicationDefinitions = null;
   private List<Application> applications = null;
   private CurrentClusterCheck clusterCheck = null;
   private MpClusterSessionFactory<ClusterInformation, SlotInformation> clusterSessionFactory = null;
   private RoutingStrategy defaultRoutingStrategy = null;
   private Serializer<Object> defaultSerializer = null;
   private Transport transport = null;
   private Map<ClusterId,Application.Cluster> allClusters = new HashMap<ClusterId, Application.Cluster>();
   private StatsCollectorFactory defaultStatsCollectorFactory = null;
   
   // Dempsy lifecycle state
   private volatile boolean isRunning = false;
   private Object isRunningEvent = new Object();
   
//   // this is mainly for testing purposes.
//   private boolean multiThreadedStart = false;
   
   public Dempsy() { }
   
   /**
    * This is meant to be autowired by type.
    */
   @Inject
   public void setApplicationDefinitions(List<ApplicationDefinition> applicationDefinitions)
   {
      this.applicationDefinitions = applicationDefinitions;
   }
   
   public synchronized void start() throws DempsyException
   {
      if (isRunning())
         throw new DempsyException("The Dempsy application " + applicationDefinitions + " has already been started." );
      
      if (applicationDefinitions == null || applicationDefinitions.size() == 0)
         throw new DempsyException("Cannot start this application because there are no ApplicationDefinitions");
      
      if (clusterSessionFactory == null)
         throw new DempsyException("Cannot start this application because there was no ClusterFactory implementaiton set.");
      
      if (clusterCheck == null)
         throw new DempsyException("Cannot start this application because there's no way to tell which cluster to start. Make sure the appropriate " + 
               CurrentClusterCheck.class.getSimpleName() + " is set.");
      
      if (defaultRoutingStrategy == null)
         throw new DempsyException("Cannot start this application because there's no default routing strategy defined.");
      
      if (defaultSerializer == null)
         throw new DempsyException("Cannot start this application because there's no default serializer defined.");
      
      if (transport == null)
         throw new DempsyException("Cannot start this application because there's no transport implementation defined");
      
      if (defaultStatsCollectorFactory == null)
        throw new DempsyException("Cannot start this application because there's no default stats collector factory defined.");
      
      applications = new ArrayList<Application>(applicationDefinitions.size()); 
      for(ApplicationDefinition appDef: this.applicationDefinitions)
      {
         appDef.initialize();
         if (clusterCheck.isThisNodePartOfApplication(appDef.getApplicationName()))
         {
            Application app = new Application(appDef);
            
            // set the default routing strategy if there isn't one already set.
            if (appDef.getRoutingStrategy() == null)
               appDef.setRoutingStrategy(defaultRoutingStrategy);
            
            if (appDef.getSerializer() == null)
               appDef.setSerializer(defaultSerializer);
            
            if (appDef.getStatsCollectorFactory() == null)
               appDef.setStatsCollectorFactory(defaultStatsCollectorFactory);
            
            applications.add(app);
         }
      }
      
      boolean clusterStarted = false;
      for (Application app : applications)
         clusterStarted = app.start();
      
      if(!clusterStarted)
      {
         throw new DempsyException("Cannot start this application because cluster defination was not found.");
      }
      // if we got to here we can assume we're started
      synchronized(isRunningEvent) { isRunning = true; }
   }
   
   public synchronized void stop()
   {
      try
      {
         for(Application app : applications)
            app.stop();
      }
      finally
      {
         // even though we may have had an exception, there's no way Dempsy
         // can be considered still "running."
         synchronized(isRunningEvent) { isRunning = false; isRunningEvent.notifyAll(); }
      }
   }

   public MpClusterSessionFactory<ClusterInformation, SlotInformation> getClusterSessionFactory()
   {
      return clusterSessionFactory;
   }

   @Inject
   public void setClusterSessionFactory(MpClusterSessionFactory<ClusterInformation, SlotInformation> clusterFactory)   {
      this.clusterSessionFactory = clusterFactory;
   }
   
   @Inject
   public void setClusterCheck(CurrentClusterCheck clusterCheck)
   {
      this.clusterCheck = clusterCheck;
   }
   
//   public void setMultithreadedStart(boolean multiThreadedStart) { this.multiThreadedStart = multiThreadedStart; }
   
   @Inject
   public void setDefaultTransport(Transport transport) { this.transport = transport; }
   
   @Inject
   public void setDefaultRoutingStrategy(RoutingStrategy defaultRoutingStrategy) { this.defaultRoutingStrategy = defaultRoutingStrategy; }

   @Inject
   public void setDefaultSerializer(Serializer<Object> defaultSerializer) { this.defaultSerializer = defaultSerializer; }
   
   @Inject
   public void setDefaultStatsCollectorFactory(StatsCollectorFactory defaultfactory) { this.defaultStatsCollectorFactory = defaultfactory; }
   
   public Application.Cluster getCluster(ClusterId clusterId)
   {
      return allClusters.get(clusterId);
   }
   
   public boolean isRunning() { return isRunning; }
   
   /**
    * Wait for Dempsy to be stopped. This is useful in a 'main' that needs to wait 
    * for an external shutdown to complete.
    * @throws InterruptedException if the waiting was interrupted.
    */
   public void waitToBeStopped() throws InterruptedException { waitToBeStopped(-1);  }
   
   /**
    * Wait for Dempsy to be stopped for the specified time.
    * @return true if Dempsy actually stopped. false if the timeout was reached. 
    * @throws InterruptedException if the waiting was interrupted.
    */
   public boolean waitToBeStopped(long timeInMillis) throws InterruptedException
   {
      synchronized(isRunningEvent)
      {
         while (isRunning)
         {
            if (timeInMillis < 0)
               isRunningEvent.wait();
            else
               isRunningEvent.wait(timeInMillis);
         }
         return !isRunning();
      }
   }
   
   private static List<Class<?>> getAcceptedMessages(ClusterDefinition clusterDef)
   {
      List<Class<?>> messageClasses = new ArrayList<Class<?>>();
      Object prototype = clusterDef.getMessageProcessorPrototype();
      if (prototype != null)
      {
         for(Method method: prototype.getClass().getMethods())
         {
            if(method.isAnnotationPresent(com.nokia.dempsy.annotations.MessageHandler.class))
            {
               for(Class<?> messageType : method.getParameterTypes())
               {
                  messageClasses.add(messageType);
               }
            }
         }
      }
      return messageClasses;
   }
   
}
