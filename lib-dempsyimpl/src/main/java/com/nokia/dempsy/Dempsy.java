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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.StatsCollectorFactory;
import com.nokia.dempsy.output.OutputExecuter;
import com.nokia.dempsy.router.CurrentClusterCheck;
import com.nokia.dempsy.router.Router;
import com.nokia.dempsy.router.RoutingStrategy;
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
          * <p>A Node is essentially an {@link MpContainer} with all of the attending infrastructure.
          * Currently a Node is instantiated within the Dempsy orchestrator as a one to one with the
          * {@link Cluster}. The following things are important to keep in mind:</p>
          * 
          * <li>There is an {@link MpContainer} local to each Node</li>
          * <li>There is a single {@link ClusterInfoSessionFactory} for all of the Node's in this 
          * {@link Dempsy} instance.</li>
          * <li>There is a unique call to the {@link ClusterInfoSessionFactory}'s createSession method
          * for each Node. For zookeeper, this means there is a separate ZooKeeper client for each Node.
          * This may change in the future.</li>
          * <li>There is a single {@link Transport} instance for ALL Nodes in this Dempsy instance</li>
          * <li>There is a separate call to the {@link Transport}'s createInbound for each Node in order
          * to obtain the {@link Receiver}.</li>
          * <li>By default, there is one executor for ALL Node's running in a Dempsy instance. This is 
          * configurable by setting a separate instance per {@link ClusterDefinition} in the 
          * {@link ApplicationDefinition}. This can be done using a factory in the DI injection 
          * container configuration or using a prototype bean scope.</li>
          * <li>There is an individual call to StatsCollectorFactory.createStatsCollector on the
          * StatsCollectorFactory for each Node.</li>
          * <li>There is a {@link Router} instance local to each Node</li>
          * <li>The {@link RoutingStrategy} defaults to the same instance for ALL Node's but can be set
          * on a per-Node bases using the {@link ClusterDefinition}.</li>
          * <li>There is a unique call to the RoutingStrategy's createInbound to obtain a
          * {@link RoutingStrategy.Inbound} instance for each Node.</li>
          */
         public class Node
         {
            protected ClusterDefinition clusterDefinition;
            
            Router router = null;
            private MpContainer container = null;
            RoutingStrategy.Inbound strategyInbound = null;
            List<Class<?>> acceptedMessageClasses = null;
            Receiver receiver = null;
            StatsCollector statsCollector = null;
            
            private Node(ClusterDefinition clusterDefinition) { this.clusterDefinition = clusterDefinition; }
            
            @SuppressWarnings("unchecked")
            private void start() throws DempsyException
            {
               if (logger.isTraceEnabled())
                   logger.trace("Starting node for " + clusterDefinition.getClusterId());

               try
               {
                  DempsyExecutor executor = (DempsyExecutor)clusterDefinition.getExecutor(); // this can be null
                  if (executor != null)
                     executor.start();
                  ClusterInfoSession clusterSession = clusterSessionFactory.createSession();
                  ClusterId currentClusterId = clusterDefinition.getClusterId();
                  router = new Router(clusterDefinition);
                  router.setClusterSession(clusterSession);
                  // get the executor if one is set
                  
                  container = new MpContainer(currentClusterId);
                  container.setDispatcher(router);
                  if (executor != null)
                     container.setExecutor(executor);
                  Object messageProcessorPrototype = clusterDefinition.getMessageProcessorPrototype();
                  if (messageProcessorPrototype != null)
                    container.setPrototype(messageProcessorPrototype);
                  acceptedMessageClasses = getAcceptedMessages(clusterDefinition);
                  
                  Serializer<Object> serializer = (Serializer<Object>)clusterDefinition.getSerializer();
                  if (serializer != null)
                     container.setSerializer(serializer);
                  
                  // there is only a reciever if we have an Mp (that is, we aren't an adaptor) and start accepting messages 
                  Destination thisDestination = null;
                  if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0)
                  {
                     receiver = transport.createInbound(executor);
                     receiver.setListener(container);
                     receiver.start();
                     thisDestination = receiver.getDestination();
                  }

                  StatsCollectorFactory statsFactory = (StatsCollectorFactory)clusterDefinition.getStatsCollectorFactory();
                  if (statsFactory != null)
                  {
                     statsCollector = statsFactory.createStatsCollector(currentClusterId, 
                           receiver != null ? thisDestination : null);
                     router.setStatsCollector(statsCollector);
                     container.setStatCollector(statsCollector);
                     if (receiver != null)
                        receiver.setStatsCollector(statsCollector);
                  }
                  
                  router.setDefaultSenderFactory(transport.createOutbound(executor, statsCollector));

                  RoutingStrategy strategy = (RoutingStrategy)clusterDefinition.getRoutingStrategy();
                  
                  KeySource<?> keySource = clusterDefinition.getKeySource();
                  if (keySource != null)
                     container.setKeySource(keySource);

                  // there is only an inbound strategy if we have an Mp (that is, we aren't an adaptor) and
                  // we actually accept messages
                  if (messageProcessorPrototype != null && acceptedMessageClasses != null && acceptedMessageClasses.size() > 0)
                     strategyInbound = strategy.createInbound(clusterSession,currentClusterId,acceptedMessageClasses, thisDestination,container);
                  
                  // this can fail because of down cluster manager server ... but it should eventually recover.
                  try { router.start(); }
                  catch (ClusterInfoException e)
                  {
                     logger.warn("Strategy failed to initialize. Continuing anyway. The cluster manager issue will be resolved automatically.",e);
                  }
                  
                  Adaptor adaptor = clusterDefinition.isRouteAdaptorType() ? clusterDefinition.getAdaptor() : null;
                  if (adaptor != null)
                     adaptor.setDispatcher(router);
                  else {
                    OutputExecuter outputExecuter = (OutputExecuter) clusterDefinition.getOutputExecuter();
                    if (outputExecuter != null) {
                       outputExecuter.setOutputInvoker(container);
                    }
                 }
                  
                  container.startEvictionThread(Cluster.this.clusterDefinition.getEvictionFrequency(), Cluster.this.clusterDefinition.getEvictionTimeUnit());
                  
               }
               catch(RuntimeException e) { throw e; }
               catch(Exception e) { throw new DempsyException(e); }
            }
            
            public StatsCollector getStatsCollector() { return statsCollector; }
            
            public MpContainer getMpContainer() { return container; }

            public void stop()
            {
               if (logger.isTraceEnabled())
                   logger.trace("Stopping node for " + clusterDefinition.getClusterId());

               if (receiver != null) 
               {
                  try { receiver.shutdown(); receiver = null; }
                  catch (Throwable th)
                  {
                     logger.error("Error stoping the reciever " + SafeString.objectDescription(receiver) + 
                           " for " + SafeString.valueOf(clusterDefinition) + " due to the following exception:",th);
                  }
               }
               
               // shut the container down prior to the router.
               if (container != null)
                  try { container.shutdown(); container = null; } catch (Throwable th) { logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
               
               // the cluster session is stopped by the router.
               if (router != null)
                  try { router.stop(); router = null; } catch (Throwable th) { logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
               
               if (statsCollector != null)
                  try { statsCollector.stop(); statsCollector = null;} catch (Throwable th) { logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }

               if (strategyInbound != null)
                  try { strategyInbound.stop(); strategyInbound = null;} catch (Throwable th) { logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
               
               DempsyExecutor executor = (DempsyExecutor)clusterDefinition.getExecutor(); // this can be null
               if (executor != null)
                  try { executor.shutdown(); } catch (Throwable th) { logger.info("Problem shutting down node for " + SafeString.valueOf(clusterDefinition), th); }
            }
            
            // Only called from tests
            public Router retouRteg() { return router; }

         } // end Node definition
         
         private List<Node> nodes = new ArrayList<Node>(1);
         protected ClusterDefinition clusterDefinition;
         
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
      
      protected ApplicationDefinition applicationDefinition;
      protected List<Cluster> appClusters = new ArrayList<Cluster>();
      private List<AdaptorThread> adaptorThreads = new ArrayList<AdaptorThread>();
      
      public Application(ApplicationDefinition applicationDefinition)
      {
         this.applicationDefinition = applicationDefinition;  
      }
      
      private DempsyException failedStart = null;
      
      /**
       * 
       * @return boolean - true if starts the cluster, else false
       * @throws DempsyException
       */
      public boolean start() throws DempsyException
      {
         if (logger.isTraceEnabled())
             logger.trace("Starting application for " + applicationDefinition.getApplicationName());
            
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
               else {
                 OutputExecuter outputExecuter = (OutputExecuter) cluster.clusterDefinition.getOutputExecuter();
                 if (outputExecuter != null) {
                    outputExecuter.start();
                 }
              }
            }
         }
         return clusterStarted;
      }
      
      public void stop()
      {
         if (logger.isTraceEnabled())
             logger.trace("Stopping application for " + applicationDefinition.getApplicationName());

         // first stop all of the adaptor threads
         for(AdaptorThread adaptorThread : adaptorThreads)
            adaptorThread.stop();

         // stop all of the non-adaptor clusters.
         for(Cluster cluster : appClusters)
         {
           OutputExecuter outputExecuter = (OutputExecuter) cluster.clusterDefinition.getOutputExecuter();
           if (outputExecuter != null) {
              outputExecuter.stop();
           }
            cluster.stop();
         }
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
   private List<ClusterDefinition> clusterDefinitions = null;
   protected Map<String,Application> applications = null;
   private CurrentClusterCheck clusterCheck = null;
   protected ClusterInfoSessionFactory clusterSessionFactory = null;
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
   public void setApplicationDefinitions(List<ApplicationDefinition> applicationDefinitions)
   {
      this.applicationDefinitions = applicationDefinitions;
   }
   
   /**
    * This is meant to be autowired by type.
    */
   public void setClusterDefinitions(List<ClusterDefinition> clusterDefinitions)
   {
      this.clusterDefinitions = clusterDefinitions;
   }
   
   private Application setupApplicationAndDefinition(ApplicationDefinition appDef) throws DempsyException
   {
      Application app = new Application(appDef);
      applications.put(appDef.getApplicationName(), app);

      // set the default routing strategy if there isn't one already set.
      if (appDef.getRoutingStrategy() == null)
         appDef.setRoutingStrategy(defaultRoutingStrategy);

      if (appDef.getSerializer() == null)
         appDef.setSerializer(defaultSerializer);

      if (appDef.getStatsCollectorFactory() == null)
         appDef.setStatsCollectorFactory(defaultStatsCollectorFactory);

      return app;
   }
   
   public synchronized void start() throws DempsyException
   {
      if (isRunning())
         throw new DempsyException("The Dempsy application " + applicationDefinitions + " has already been started." );
      
      if ((applicationDefinitions == null || applicationDefinitions.size() == 0) && 
            (clusterDefinitions == null || clusterDefinitions.size() == 0))
         throw new DempsyException("Cannot start this application because there are no ApplicationDefinitions");
      
      if (clusterSessionFactory == null)
         throw new DempsyException("Cannot start this application because there was no ClusterInfoSessionFactory implementaiton set.");
      
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
    
      try
      {
         applications = new HashMap<String,Application>();
         
         if (applicationDefinitions != null)
         {
            for(ApplicationDefinition appDef: this.applicationDefinitions)
            {
               Application app = applications.get(appDef.getApplicationName());
               if (app == null)
               {
                  if (clusterCheck.isThisNodePartOfApplication(appDef.getApplicationName()))
                     app = setupApplicationAndDefinition(appDef);
               }
               else
                  throw new DempsyException("You cannot have two instances of an ApplicationDefinition for the same application. " +
                        "You appear to have two different ApplicationDefinitions for " + SafeString.valueOf(appDef));
            }
         }
         
         if (clusterDefinitions != null)
         {
            for(ClusterDefinition clusterDef: this.clusterDefinitions)
            {
               ApplicationDefinition appDef = clusterDef.getApplicationDefinition();
               if (appDef == null)
                  throw new DempsyException("The dynamic cluster defnition " + SafeString.valueOf(clusterDef) + 
                        " requires a reference to an ApplicationDefinition but none was provided." + 
                        " Please make sure the ApplicationDefinition for this cluster.");
               
               appDef.addDynamicClusterDefinition(clusterDef);
               
               if (clusterCheck.isThisNodePartOfApplication(appDef.getApplicationName()))
               {
                  Application app = applications.get(appDef.getApplicationName());
                  
                  if (app == null)
                     setupApplicationAndDefinition(appDef);
                  else
                  {
                     // verify this is the exact same instance of the appDef
                     if (app.applicationDefinition != appDef)
                        throw new DempsyException("You cannot have two instances of an ApplicationDefinition for the same application. " +
                              "You appear to have two different ApplicationDefinitions for " + SafeString.valueOf(appDef) + 
                              ". One is referenced in the ClusterDefinition " + SafeString.valueOf(clusterDef));
                  }
                  
               }
            }
         }
         
         for (Application app : applications.values())
            app.applicationDefinition.initialize();
         
         boolean clusterStarted = false;
         for (Application app : applications.values())
            clusterStarted = app.start();

         if(!clusterStarted)
            throw new DempsyException("Cannot start this application because cluster definition was not found.");

         // if we got to here we can assume we're started
         synchronized(isRunningEvent) { isRunning = true; }
      }
      catch (RuntimeException rte)
      {
         logger.error("Failed to start Dempsy. Attempting to stop.");
         // if something unpexpected happened then we should attempt to stop
         try { stop(); } catch (Throwable th) {}
         throw rte;
      }
   }
   
   public synchronized void stop()
   {
      try
      {
         for(Application app : applications.values())
            app.stop();
      }
      finally
      {
         // even though we may have had an exception, there's no way Dempsy
         // can be considered still "running."
         synchronized(isRunningEvent) { isRunning = false; isRunningEvent.notifyAll(); }
      }
   }

   public ClusterInfoSessionFactory getClusterSessionFactory()
   {
      return clusterSessionFactory;
   }

   public void setClusterSessionFactory(ClusterInfoSessionFactory clusterFactory)   {
      this.clusterSessionFactory = clusterFactory;
   }
   
   public void setClusterCheck(CurrentClusterCheck clusterCheck)
   {
      this.clusterCheck = clusterCheck;
   }
   
//   public void setMultithreadedStart(boolean multiThreadedStart) { this.multiThreadedStart = multiThreadedStart; }
   
   public void setDefaultTransport(Transport transport) { this.transport = transport; }
   
   public void setDefaultRoutingStrategy(RoutingStrategy defaultRoutingStrategy) { this.defaultRoutingStrategy = defaultRoutingStrategy; }

   public void setDefaultSerializer(Serializer<Object> defaultSerializer) { this.defaultSerializer = defaultSerializer; }

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
      boolean traceEnabled = logger.isTraceEnabled();
      if (traceEnabled)
         logger.trace("Waiting for Dempsy to stop.");

      synchronized(isRunningEvent)
      {
         while (isRunning)
         {
            if (timeInMillis < 0)
               isRunningEvent.wait();
            else
               isRunningEvent.wait(timeInMillis);
         }

         if (traceEnabled)
            logger.trace("Dempsy is stopped.");

         return !isRunning();
      }

   }
   
   protected static List<Class<?>> getAcceptedMessages(ClusterDefinition clusterDef)
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
