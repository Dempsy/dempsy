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

import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.monitoring.coda.MetricGetters;

public class TestDempsy
{
   private static Logger logger = LoggerFactory.getLogger(TestDempsy.class);
   
   private static long baseTimeoutMillis = 20000; // 20 seconds
   
   String[] dempsyConfigs = new String[] { "testDempsy/Dempsy.xml" };
   
   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/ClusterInfo-LocalActx.xml" };
   String[][] transports = new String[][] {
         { "testDempsy/Transport-PassthroughActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml" }, 
         { "testDempsy/Transport-BlockingQueueActx.xml" }, 
         { "testDempsy/Transport-TcpActx.xml", "testDempsy/Transport-TcpWithOverflowActx.xml" }
   };
   
   // bad combinations.
   List<ClusterId> badCombos = Arrays.asList(new ClusterId[] {
         // this is a hack ... use a ClusterId as a String tuple for comparison
         
         // the passthrough Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughActx.xml") , 
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml") , 
         
         // the blockingqueue Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-BlockingQueueActx.xml") 
   });
 
   private static InitZookeeperServerBean zkServer = null;

   @BeforeClass
   public static void setupZookeeperSystemVars() throws IOException
   {
      System.setProperty("application", "test-app");
      System.setProperty("cluster", "test-cluster2");
      zkServer = new InitZookeeperServerBean();
   }
   
   @AfterClass
   public static void shutdownZookeeper()
   {
      zkServer.stop();
   }
   
   @Before
   public void init()
   {
      KeySourceImpl.disruptSession = false;
      KeySourceImpl.infinite = false;
      KeySourceImpl.pause = new CountDownLatch(0);
   }
   
   public static class TestMessage implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private String val;
      
      public TestMessage(String val) { this.val = val; }
      
      @MessageKey
      public String get() { return val; } 
      
      public boolean equals(Object o) 
      {
         return o == null ? false :
            String.valueOf(val).equals(String.valueOf(((TestMessage)o).val)); 
      }
   }
   
   @MessageProcessor
   public static class TestMp implements Cloneable
   {
      // need a mutable object reference
      public AtomicReference<TestMessage> lastReceived = new AtomicReference<TestMessage>();
      public AtomicLong outputCount = new AtomicLong(0);
      public CountDownLatch outputLatch = new CountDownLatch(10);
      public AtomicInteger startCalls = new AtomicInteger(0);
      public AtomicInteger cloneCalls = new AtomicInteger(0);
      
      @Start
      public void start()
      {
         startCalls.incrementAndGet();
      }
      
      @MessageHandler
      public void handle(TestMessage message)
      {
         lastReceived.set(message);
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
         System.out.println("Overflow:" + messageBytes);
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
         lastSent = message;
         dispatcher.dispatch(message);
      }
   }
   
   public static class KeySourceImpl implements KeySource<String>
   {
      private Dempsy dempsy = null;
      private ClusterId clusterId = null;
      public static volatile boolean disruptSession = false;
      public static volatile boolean infinite = false;
      public static volatile CountDownLatch pause = new CountDownLatch(0);
      public static volatile KSIterable lastCreated = null;
      
      public void setDempsy(Dempsy dempsy) { this.dempsy = dempsy; }
      
      public void setClusterId(ClusterId clusterId) { this.clusterId = clusterId; }
      
      public class KSIterable implements Iterable<String>
      {
         public volatile String lastKey = "";
         public CountDownLatch m_pause = pause;
         public volatile boolean m_infinite = infinite;

         {
            lastCreated = this;
         }

         @Override
         public Iterator<String> iterator()
         {
            return new Iterator<String>()
            {
               long count = 0;

               @Override
               public boolean hasNext() { if (count >= 1) kickClusterInfoMgr(); return m_infinite ? true : (count < 2);  }

               @Override
               public String next() { try { m_pause.await(); } catch (InterruptedException ie) {} count++; return (lastKey = "test" + count);}

               @Override
               public void remove() { throw new UnsupportedOperationException(); }

               private void kickClusterInfoMgr() 
               {
                  if (!disruptSession)
                     return;
                  disruptSession = false; // one disruptSession
                  Dempsy.Application.Cluster c = dempsy.getCluster(clusterId);
                  Object session = TestUtils.getSession(c);
                  if (session instanceof DisruptibleSession)
                  {
                     DisruptibleSession dses = (DisruptibleSession)session;
                     dses.disrupt();
                  }
               }
            };
         }
         
      }
      
      @Override
      public Iterable<String> getAllPossibleKeys()
      {
         // The array is proxied to create the ability to rip out the cluster manager
         // in the middle of iterating over the key source. This is to create the 
         // condition in which the key source is being iterated while the routing strategy
         // is attempting to get slots.
         return new KSIterable();
      }
   }
   
   public static interface Checker
   {
      public void check(ApplicationContext context) throws Throwable;
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
   
   public void runAllCombinations(String applicationContext, Checker checker) throws Throwable
   {
      int runCount = 0;
      for (String clusterManager : clusterManagers)
      {
         for (String[] alternatingTransports : transports)
         {
            // select one of the alternatingTransports
            String transport = alternatingTransports[runCount % alternatingTransports.length];

            // alternate the dempsy configs
            String dempsyConfig = dempsyConfigs[runCount % dempsyConfigs.length];

            if (! badCombos.contains(new ClusterId(clusterManager,transport)))
            {
               String pass = " test: " + (checker == null ? "none" : checker) + " using " + dempsyConfig + "," + clusterManager + "," + transport;
               try
               {
                  logger.debug("*****************************************************************");
                  logger.debug(pass);
                  logger.debug("*****************************************************************");

                  String[] ctx = { dempsyConfig, clusterManager, transport, "testDempsy/" + applicationContext };

                  logger.debug("Starting up the appliction context ...");
                  ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(ctx);
                  actx.registerShutdownHook();
                  
                  Dempsy dempsy = (Dempsy)actx.getBean("dempsy");
                  
                  assertTrue(pass,TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, 20, dempsy));

                  WaitForShutdown waitingForShutdown = new WaitForShutdown(dempsy);
                  Thread waitingForShutdownThread = new Thread(waitingForShutdown,"Waiting For Shutdown");
                  waitingForShutdownThread.start();
                  Thread.yield();

                  logger.debug("Running test ...");
                  if (checker != null)
                     checker.check(actx);
                  logger.debug("Done with test, stopping the application context ...");

                  actx.stop();
                  actx.destroy();

                  assertTrue(waitingForShutdown.waitForShutdownDoneLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                  assertTrue(waitingForShutdown.shutdown);

                  logger.debug("Finished this pass.");
               }
               catch (AssertionError re)
               {
                  logger.error("***************** FAILED ON: " + pass);
                  throw re;
               }
               
               runCount++;
            }
         }
      }
   }
   
   @Test 
   public void testIndividualClusterStart() throws Throwable
   {
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-IndividualClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/SimpleMultistageApplicationActx.xml"
            );
      actx.registerShutdownHook();
      
      Dempsy dempsy = (Dempsy)actx.getBean("dempsy");
      assertNotNull(dempsy);
      
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId("test-app", "test-cluster0"));
      assertNull(cluster);

      cluster = dempsy.getCluster(new ClusterId("test-app", "test-cluster1"));
      assertNull(cluster);

      cluster = dempsy.getCluster(new ClusterId("test-app", "test-cluster2"));
      assertNotNull(cluster);
      assertEquals(1,cluster.getNodes().size());

      cluster = dempsy.getCluster(new ClusterId("test-app", "test-cluster3"));
      assertNull(cluster);

      cluster = dempsy.getCluster(new ClusterId("test-app", "test-cluster4"));
      assertNull(cluster);

      actx.stop();
      actx.destroy();
   }

   @Test(expected=BeanCreationException.class) 
   public void testInValidClusterStart() throws Throwable
   {
      new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-InValidClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/SimpleMultistageApplicationActx.xml"
            );
   }

   @Test
   public void testAdaptorThrowsRuntimeOnSetDispatcher() throws Throwable
   {
      TestAdaptor.throwExceptionOnSetDispatcher = true;
      ClassPathXmlApplicationContext actx = null;
      boolean gotException = false;
      
      try
      {
         actx = new ClassPathXmlApplicationContext(
               "testDempsy/Dempsy.xml",
               "testDempsy/Transport-PassthroughActx.xml",
               "testDempsy/ClusterInfo-LocalActx.xml",
               "testDempsy/SimpleMultistageApplicationActx.xml"
               );
         actx.registerShutdownHook();
      }
      catch (Throwable th)
      {
         assertEquals("Forced RuntimeException",th.getCause().getLocalizedMessage());
         gotException = true;
      }
      finally
      {
         TestAdaptor.throwExceptionOnSetDispatcher = false;
         if (actx != null)
         {
            actx.stop();
            actx.destroy();
         }
         
      }
      
      assertTrue(gotException);
   }
   

   @Test
   public void testStartupShutdown() throws Throwable
   {
      runAllCombinations("SimpleMultistageApplicationActx.xml",null);
   }
   
   @Test
   public void testMpStartMethod() throws Throwable
   {
      runAllCombinations("SinglestageApplicationActx.xml",
          new Checker()   
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  Object message = new Object();

                  // start things and verify that the init method was called
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                  assertEquals(1, mp.startCalls.get());
                      
                  // now send a message through
                      
                  message = new TestMessage("HereIAm - testMPStartMethod");
                  adaptor.pushMessage(message);
                      
                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  final Object msg = message;
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return msg.equals(mp.lastReceived.get()); } }));
                      
                  // verify we haven't called it again, not that there's really
                  // a way to given the code
                  assertEquals(1, mp.startCalls.get());
               }
               
               public String toString() { return "testMPStartMethod"; }
            });
   }   
   @Test
   public void testMessageThrough() throws Throwable
   {
      runAllCombinations("SinglestageApplicationActx.xml",
            new Checker()
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  Object message = new Object();
                  adaptor.pushMessage(message);
                  
                  // check that the message didn't go through.
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                  assertTrue(mp.lastReceived.get() == null);
                  
                  TestAdaptor adaptor2 = (TestAdaptor)getAdaptor(dempsy, "test-app","test-cluster0");
                  assertEquals(adaptor,adaptor2);
                  
                  assertEquals(adaptor.lastSent, message);
                  
                  // now send a message through
                  
                  message = new TestMessage("HereIAm - testMessageThrough");
                  adaptor.pushMessage(message);
                  
                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  final Object msg = message;
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return msg.equals(mp.lastReceived.get()); } }));
                  
                  assertEquals(adaptor2.lastSent,message);
                  assertEquals(adaptor2.lastSent,mp.lastReceived.get());
                  
               }
               
               public String toString() { return "testMessageThrough"; }
            });
   }
   
   @Test
   public void testOutPutMessage() throws Throwable
   {
      runAllCombinations("SinglestageOutputApplicationActx.xml",
            new Checker()
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  TestMessage message = new TestMessage("output");
                  adaptor.pushMessage(message); // this causes the container to clone the Mp
                  
                  // Now wait for the output call to be made 10 times (or so).
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                  assertTrue(mp.outputLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                  assertTrue(mp.outputCount.get()>=10);
               }
               
               public String toString() { return "testOutPutMessage"; }

            });
   }


   @Test
   public void testCronOutPutMessage() throws Throwable
   {
      runAllCombinations("SinglestageOutputApplicationActx.xml",
            new Checker()
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  TestMessage message = new TestMessage("output");
                  adaptor.pushMessage(message); // this causes the container to clone the Mp
                  
                  // Now wait for the output call to be made 10 times (or so).
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster2");
                  assertTrue(mp.outputLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                  assertTrue(mp.outputCount.get()>=10);
               }
               
               public String toString() { return "testCronOutPutMessage"; }

            });
   }

   @Test
   public void testExplicitDesintationsStartup() throws Throwable
   {
      runAllCombinations("MultistageApplicationExplicitDestinationsActx.xml",
            new Checker()
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
               }
               
               public String toString() { return "testExplicitDesintationsStartup"; }

            });
   }
   
   private static Object getMp(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0); // currently there is one node per cluster.
      return node.clusterDefinition.getMessageProcessorPrototype();
   }
   
   private static Adaptor getAdaptor(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0); // currently there is one node per cluster.
      return node.clusterDefinition.getAdaptor();
   }

   @Test
   public void testMpKeyStore() throws Throwable
   {
      runMpKeyStoreTest("testMpKeyStore");
   }
   
   @Test
   public void testMpKeyStoreWithFailingClusterManager() throws Throwable
   {
      KeySourceImpl.disruptSession = true;
      runMpKeyStoreTest("testMpKeyStoreWithFailingClusterManager");
   }
   
   public void runMpKeyStoreTest(final String methodName) throws Throwable
   {
      runAllCombinations("SinglestageWithKeyStoreApplicationActx.xml",
          new Checker()   
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  // start things and verify that the init method was called
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                      
                  // verify we haven't called it again, not that there's really
                  // a way to given the code
                  assertEquals(1, mp.startCalls.get());

                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==2; } }));
                  
                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  adaptor.pushMessage(new TestMessage("output")); // this causes the container to clone the Mp

                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==3; } }));
                  
                  adaptor.pushMessage(new TestMessage("test1")); // this causes the container to clone the Mp

                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==3; } }));
                  List<Node> nodes = dempsy.getCluster(new ClusterId("test-app","test-cluster1")).getNodes();
                  Assert.assertNotNull(nodes);
                  Assert.assertTrue(nodes.size()>0);
                  Node node = nodes.get(0);
                  Assert.assertNotNull(node);
                  double duration = ((MetricGetters)node.getStatsCollector()).getPreInstantiationDuration();
                  Assert.assertTrue(duration>0.0);
               }
               
               public String toString() { return methodName; }
            });
   }
   
   @Test
   public void testOverlappingKeyStoreCalls() throws Throwable
   {
      KeySourceImpl.pause = new CountDownLatch(1);
      KeySourceImpl.infinite = true;

      runAllCombinations("SinglestageWithKeyStoreApplicationActx.xml",
          new Checker()   
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  // wait until the KeySourceImpl has been created
                  assertTrue(poll(baseTimeoutMillis,null,new Condition<Object>() { @Override public boolean conditionMet(Object mp) {  return KeySourceImpl.lastCreated != null; } }));
                  final KeySourceImpl.KSIterable firstCreated = KeySourceImpl.lastCreated;
                  
                  // start things and verify that the init method was called
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");

                  Dempsy.Application.Cluster c = dempsy.getCluster(new ClusterId("test-app","test-cluster1"));
                  assertNotNull(c);
                  Dempsy.Application.Cluster.Node node = c.getNodes().get(0);
                  assertNotNull(node);
                  
                  MpContainer container = node.getMpContainer();
                  
                  // let it go and wait until there's a few keys.
                  firstCreated.m_pause.countDown();
                  
                  // as the KeySource iterates, this will increase
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get() > 10000; } }));

                  // prepare the next countdown latch
                  KeySourceImpl.pause = new CountDownLatch(0); // just let the 2nd one go
                  
                  // I want the next one to stop at 2
                  KeySourceImpl.infinite = false;

                  // Now force another call while the first is running
                  container.keyspaceResponsibilityChanged(node.strategyInbound, false, true);
                  
                  // wait until the second one is created
                  assertTrue(poll(baseTimeoutMillis,null,new Condition<Object>() { @Override public boolean conditionMet(Object mp) {  return KeySourceImpl.lastCreated != null && firstCreated != KeySourceImpl.lastCreated; } }));
                  
                  // now the first one should be done and therefore no longer incrementing.
                  String lastKeyOfFirstCreated = firstCreated.lastKey;

                  // and the second one should be done also and stopped at 2.
                  final KeySourceImpl.KSIterable secondCreated = KeySourceImpl.lastCreated;
                  assertTrue(firstCreated != secondCreated);
                  
                  assertTrue(poll(baseTimeoutMillis,null,new Condition<Object>() { @Override public boolean conditionMet(Object mp) {  return "test2".equals(secondCreated.lastKey); } }));
                  
                  Thread.sleep(50);
                  assertEquals(lastKeyOfFirstCreated,firstCreated.lastKey); // make sure the first one isn't still moving on
                  assertEquals("test2",secondCreated.lastKey);
                  
                  // prepare for the next run
                  KeySourceImpl.pause = new CountDownLatch(1);
                  KeySourceImpl.infinite = true;
               }
               
               public String toString() { return "testOverlappingKeyStoreCalls"; }
            });
   }   
}
