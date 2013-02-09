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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperSessionFactory;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;
import com.nokia.dempsy.util.Pair;

public class DempsyTestBase
{
   /**
    * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in 
    * every runAllCombinations call. This can make tests run for a loooooong time.
    */
   public static boolean hardcore = false;

   protected static Logger logger;
   protected static long baseTimeoutMillis = 20000; // 20 seconds

   public String[] dempsyConfigs = new String[] { "testDempsy/Dempsy.xml" };

   public String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/ClusterInfo-LocalActx.xml" };
   public String[][] transports = new String[][] {
         { "testDempsy/Transport-PassthroughActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml" }, 
         { "testDempsy/Transport-BlockingQueueActx.xml" }, 
         { "testDempsy/Transport-TcpActx.xml", "testDempsy/Transport-TcpFailSlowActx.xml", "testDempsy/Transport-TcpWithOverflowActx.xml", "testDempsy/Transport-TcpBatchedOutputActx.xml" }
   };

   public String[] serializers = new String[]
         { "testDempsy/Serializer-JavaActx.xml", "testDempsy/Serializer-KryoActx.xml", "testDempsy/Serializer-KryoOptimizedActx.xml" };

   public String[] routingStrategies = new String[]
         { "testDempsy/RoutingStrategy-DecentralizedActx.xml" };
   
   // bad combinations.
   @SuppressWarnings("unchecked")
   public List<Pair<String,String>> badCombos = Arrays.asList((Pair<String,String>[])new Pair[] {
         // the passthrough Destination is not serializable but zookeeper requires it to be
         new Pair<String,String>("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughActx.xml") , 
         new Pair<String,String>("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml") , 

         // the blockingqueue Destination is not serializable but zookeeper requires it to be
         new Pair<String,String>("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-BlockingQueueActx.xml") 
   });
   
   public static String defaultClusterCheck;

   public static InitZookeeperServerBean zkServer = null;

   @BeforeClass
   public static void setupZookeeperSystemVars() throws IOException
   {
      System.setProperty("application", "test-app");
      System.setProperty("cluster", "test-cluster2");
      zkServer = new InitZookeeperServerBean();
      
      // check for the system property that will set the hardcore flag to true
      if (System.getProperties().containsKey("test.hardcore"))
         hardcore = true;
      
      if (hardcore)
         System.out.println("Hardcore testing in progress. This will take a while, you might as well go get a coffee.");
   }

   @AfterClass
   public static void shutdownZookeeper()
   {
      zkServer.stop();
      TestZookeeperSessionFactory.useSingletonSession = false;
   }
   
   public static class TestZookeeperSessionFactory extends ZookeeperSessionFactory
   {
      public TestZookeeperSessionFactory(String connectString, int sessionTimeout)
      {
         super(connectString, sessionTimeout);
         if (useSingletonSession)
            setUseSingletonSession(useSingletonSession);
      }
      
      public static boolean useSingletonSession = false;
   }

   @Before
   public void init()
   {
      TestMp.currentOutputCount = 10;
      TestMp.activateCheckedException = false;
      TestMp.alwaysPauseOnActivation = false;
      System.setProperty("nodecount","1");
      TestZookeeperSessionFactory.useSingletonSession = false;
      defaultClusterCheck = "testDempsy/ClusterCheck-AlwaysInCurrentCluster.xml";
      TestKryoOptimizer.proxy = null;
   }

   public static class TestMessage implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private String val;

      @SuppressWarnings("unused") // required for Kryo
      private TestMessage() {} 

      public TestMessage(String val) { this.val = val; }

      @MessageKey
      public String get() { return val; } 

      public boolean equals(Object o) 
      {
         return o == null ? false :
            String.valueOf(val).equals(String.valueOf(((TestMessage)o).val)); 
      }
      
      public String toString() { return "{" + val + "}"; }
   }

   public static class TestKryoOptimizer implements KryoOptimizer
   {
      public static KryoOptimizer proxy = null;
      
      @Override
      public void preRegister(Kryo kryo)
      {
         if (proxy != null)
            proxy.preRegister(kryo);
         else
            kryo.setRegistrationRequired(true);
      }

      @Override
      public void postRegister(Kryo kryo)
      {
         if (proxy != null)
            proxy.postRegister(kryo);
         else
         {
            @SuppressWarnings("unchecked")
            FieldSerializer<TestMessage> valSer = (FieldSerializer<TestMessage>)kryo.getSerializer(TestMessage.class);
            valSer.setFieldsCanBeNull(false);
         }
      }

   }

   public static class ActivateCheckedException extends Exception
   {
      private static final long serialVersionUID = 1L;
      public ActivateCheckedException(String message) { super(message); }
   }
   
   @MessageProcessor
   public static class TestMp implements Cloneable
   {
      public static int currentOutputCount = 10;

      // need a mutable object reference
      public static volatile boolean alwaysPauseOnActivation = false;
      public AtomicReference<TestMessage> lastReceived = new AtomicReference<TestMessage>();
      public AtomicLong outputCount = new AtomicLong(0);
      public CountDownLatch outputLatch = new CountDownLatch(currentOutputCount);
      public AtomicInteger startCalls = new AtomicInteger(0);
      public AtomicInteger cloneCalls = new AtomicInteger(0);
      public AtomicLong handleCalls = new AtomicLong(0);
      public static AtomicLong globalHandleCalls = new AtomicLong(0);
      public AtomicReference<String> failASingleActivationForThisKey = new AtomicReference<String>();
      public AtomicBoolean haveWaitedOnce = new AtomicBoolean(false);
      public static boolean activateCheckedException = false;

      @Start
      public void start()
      {
         startCalls.incrementAndGet();
      }

      @MessageHandler
      public void handle(TestMessage message)
      {
         lastReceived.set(message);
         handleCalls.incrementAndGet();
         globalHandleCalls.incrementAndGet();
      }

      @Activation
      public void setKey(String key) throws ActivateCheckedException
      {
         // we need to wait at least once because sometime pre-instantiation 
         // goes so fast the test fails because it fails to register on the statsCollector.
         if (!haveWaitedOnce.get() || alwaysPauseOnActivation)
         {
            try { Thread.sleep(3); } catch (Throwable th) {}
            haveWaitedOnce.set(true);
         }

         if (key.equals(failASingleActivationForThisKey.get()))
         {
            failASingleActivationForThisKey.set(null); // reset the key
            String message = "Failed Activation For " + key;
            if (activateCheckedException)
               throw new ActivateCheckedException(message);
            else
               throw new RuntimeException(message);
         }
      }

      @Override
      public TestMp clone() throws CloneNotSupportedException 
      {
         cloneCalls.incrementAndGet();
         return (TestMp) super.clone();
      }

      @Output
      public void output()
      {
         outputCount.incrementAndGet();
         outputLatch.countDown();
      }
   }

   public static class OverflowHandler implements com.nokia.dempsy.messagetransport.OverflowHandler
   {

      @Override
      public void overflow(byte[] messageBytes)
      {
         logger.debug("Overflow:" + messageBytes);
      }

   }

   public static class TestAdaptor implements Adaptor
   {
      Dispatcher dispatcher;
      public Object lastSent;
      public volatile static boolean throwExceptionOnSetDispatcher = false; 

      @Override
      public void setDispatcher(Dispatcher dispatcher)
      {
         this.dispatcher = dispatcher;
         if (throwExceptionOnSetDispatcher) throw new RuntimeException("Forced RuntimeException"); 
      }

      @Override
      public void start() { }

      @Override
      public void stop() { }

      public void pushMessage(Object message)
      {
         logger.trace(TestAdaptor.class.getSimpleName() + " is pushing " + SafeString.objectDescription(message));
         lastSent = message;
         dispatcher.dispatch(message);
      }
   }

   public static abstract class Checker
   {
      public void check(ClassPathXmlApplicationContext[] context) throws Throwable { check(context[0]); }

      public void check(ClassPathXmlApplicationContext context) throws Throwable { throw new RuntimeException("I shouldn't be here"); }
      
      public void setup() throws Throwable {}
   }

   private static class WaitForShutdown implements Runnable
   {

      public boolean shutdown = false;
      public Dempsy dempsy = null;
      public CountDownLatch waitForShutdownDoneLatch = new CountDownLatch(1);

      WaitForShutdown(Dempsy dempsy) { this.dempsy = dempsy; }

      @Override
      public void run()
      {
         try { dempsy.waitToBeStopped(); shutdown = true; } catch(InterruptedException e) {}
         waitForShutdownDoneLatch.countDown();
      }

   }

   static class AlternatingIterable implements Iterable<String>
   {
      boolean hardcore = false;
      List<String> strings = null;

      public AlternatingIterable(boolean hardcore, String[] strings)
      {
         this.hardcore = hardcore; 
         this.strings = Arrays.asList(strings);
      }

      @Override
      public Iterator<String> iterator()
      {
         return hardcore ? strings.iterator() : 
            new Iterator<String>()
            {
            boolean done = false;

            @Override
            public boolean hasNext() { return !done; }

            @Override
            public String next(){ done = true; return strings.get(runCount % strings.size()); }

            @Override
            public void remove() { throw new UnsupportedOperationException(); }
            };
      }

   }

   static int runCount = 0;
   public void runAllCombinations(Checker checker, String... applicationContexts) throws Throwable
   {
      String[][] param = new String[1][];
      param[0] = applicationContexts;
      runAllCombinations(checker,param);
   }

   public void runAllCombinations(Checker checker, String[]... applicationContextsArray) throws Throwable
   {
      
      @SuppressWarnings("unchecked")
      Pair<String[],String>[] param = (Pair<String[],String>[])new Pair[applicationContextsArray.length];
      int index = 0;
      for (String[] curAppCtx : applicationContextsArray)
         param[index++] = new Pair<String[],String>(curAppCtx,null);
      runAllCombinations(checker,param);
   }
   
   private Pair<String[],String>[] currentlyRunningClusterDescriptions = null;
   private String curClusterManager;
   private String curTransport;
   private String curSerializer;
   private String curDempsyConfig;
   private String curRoutingStrategy;
   List<ClassPathXmlApplicationContext> curContexts;
   
   private synchronized ClassPathXmlApplicationContext startDempsy(Pair<String[],String> desc)
   {
      int count = 6;
      String[] applicationContexts = desc.getFirst();
      String[] ctx = new String[count + applicationContexts.length];
      ctx[0] = curDempsyConfig; ctx[1] = curClusterManager; ctx[2] = curTransport; ctx[3] = curSerializer; ctx[4] = curRoutingStrategy; 

      String clusterIndicator = desc.getSecond();
      if (clusterIndicator == null)
         ctx[5] = defaultClusterCheck;
      else
      {
         ctx[5] = "testDempsy/ClusterCheck-RegExClusterCheck.xml";
         System.setProperty("clustermatch.regexp",clusterIndicator);
      }

      for (String appctx : applicationContexts)
         ctx[count++] = "testDempsy/" + appctx;

      logger.debug("Starting up the appliction context " + Arrays.asList(ctx) );
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(ctx);
      actx.registerShutdownHook();
      return actx;
   }

   public synchronized ClassPathXmlApplicationContext startAnotherNode(int dempsyIndex)
   {
      if (currentlyRunningClusterDescriptions == null)
         throw new RuntimeException("There doesn't appear to be a currently executing runAllCombinations test.");
      
      ClassPathXmlApplicationContext actx = startDempsy(currentlyRunningClusterDescriptions[dempsyIndex]);
      curContexts.add(actx);
      return actx;
   }

   /**
    * <p>This mega integration test driver allows for the creation of an entire distributed application
    * within this JVM. The clusterDescriptions need some ... description.</p>
    * 
    * <p>This clusterDescriptions parameter provides fine grained behavior over what get's instantiated
    * per node. The first String[] in the outer Pair is a set of xml files that make up a single
    * Spring application contest. With each Spring application context you can provide a regexpr for the 
    * ClusterCheck to select out the appropriate Cluster for the node being instantiated.</p>
    */
   public synchronized void runAllCombinations(Checker checker, Pair<String[],String>... clusterDescriptions) throws Throwable
   {
      if (currentlyRunningClusterDescriptions != null)
         throw new RuntimeException("Cannot start another test before finishing the previous one.");
      currentlyRunningClusterDescriptions = clusterDescriptions;
      
      try
      {
         for (String clusterManager : clusterManagers)
         {
            curClusterManager = clusterManager;
            for (String[] alternatingTransports : transports)
            {
               // select one of the alternatingTransports
               for (String transport : new AlternatingIterable(hardcore,alternatingTransports))
               {
                  curTransport = transport;
                  for (String serializer : new AlternatingIterable(hardcore,serializers))
                  {
                     curSerializer = serializer;
                     // alternate the dempsy configs
                     for (String dempsyConfig : new AlternatingIterable(hardcore,dempsyConfigs))
                     {
                        curDempsyConfig = dempsyConfig;
                        for (String routingStrategy : routingStrategies)
                        {
                           curRoutingStrategy = routingStrategy;
                           
                           if (! badCombos.contains(new Pair<String,String>(curClusterManager,curTransport)))
                           {
                              // for the sake of the 'pass' string we need to convert the String[][] to a list of lists.
                              List<List<String>> tpassname = new ArrayList<List<String>>();
                              for (Pair<String[],String> cur : clusterDescriptions)
                                 tpassname.add(Arrays.asList(cur.getFirst()));
                              String pass = tpassname.toString() + " test: " + (checker == null ? "none" : checker) + " using " + 
                                    curDempsyConfig + "," + curClusterManager + "," + curSerializer + "," + curTransport + "," + curRoutingStrategy;

                              curContexts = new ArrayList<ClassPathXmlApplicationContext>(clusterDescriptions.length);
                              WaitForShutdown[] shutdownWaits = new WaitForShutdown[clusterDescriptions.length];
                              Dempsy[] dempsys = new Dempsy[clusterDescriptions.length];
                              int dempsyCount = 0;

                              try
                              {
                                 logger.debug("*****************************************************************");
                                 logger.debug(pass);
                                 logger.debug("*****************************************************************");

                                 if (checker != null)
                                    checker.setup();

                                 // instantiate each Dempsy
                                 for (Pair<String[],String> desc : clusterDescriptions)
                                 {
                                    ClassPathXmlApplicationContext actx = startDempsy(desc);
                                    Dempsy dempsy = (Dempsy)actx.getBean("dempsy");
                                    dempsys[dempsyCount] = dempsy;
                                    curContexts.add(actx);
                                    dempsyCount++;
                                 }

                                 dempsyCount = 0;
                                 for (Dempsy dempsy : dempsys)
                                 {
                                    assertTrue(pass,TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy));

                                    WaitForShutdown waitingForShutdown = new WaitForShutdown(dempsy);
                                    Thread waitingForShutdownThread = new Thread(waitingForShutdown,"Waiting For Shutdown");
                                    waitingForShutdownThread.start();
                                    shutdownWaits[dempsyCount++] = waitingForShutdown;
                                 }

                                 logger.debug("Running test ...");
                                 if (checker != null)
                                    checker.check(curContexts.toArray(new ClassPathXmlApplicationContext[0]));
                                 logger.debug("Done with test, stopping the application context ...");

                                 logger.debug("Finished this pass.");
                              }
                              catch (AssertionError re)
                              {
                                 logger.error("***************** FAILED ON: " + pass);
                                 throw re;
                              }
                              finally
                              {
                                 try
                                 {
                                    for (ClassPathXmlApplicationContext cur : curContexts)
                                    {
                                       ClassPathXmlApplicationContext actx = cur;
                                       if (actx != null)
                                       {
                                          actx.stop();
                                          actx.destroy();
                                       }
                                    }

                                    for (WaitForShutdown waitingForShutdown : shutdownWaits)
                                    {
                                       if (waitingForShutdown != null)
                                       {
                                          assertTrue(waitingForShutdown.waitForShutdownDoneLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                                          assertTrue(waitingForShutdown.shutdown);
                                       }
                                    }
                                 }
                                 catch (Throwable th)
                                 {
                                    logger.error("FAILED TO SHUT DOWN TEST. SUBSEQUENT TESTS MAY BE CORRUPTED!",th);
                                 }
                              }

                              runCount++;
                           }
                        }
                     }
                  }
               }
            }
         }
      }
      finally
      {
         currentlyRunningClusterDescriptions = null;
         curClusterManager = null;
         curTransport = null;
         curSerializer = null;
         curDempsyConfig = null;
         curRoutingStrategy = null;
         curContexts = null;
      }
   }
}

