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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;

public class TestDempsy
{
   private static Logger logger = LoggerFactory.getLogger(TestDempsy.class);
   
   private static long baseTimeoutMillis = 20000; // 20 seconds
   
   String[] dempsyConfigs = new String[] { "testDempsy/Dempsy.xml" /*, "testDempsy/Dempsy-MultiThreadedStartup.xml"*/ };
   
   String[] clusterManagers = new String[]{ "testDempsy/ClusterManager-ZookeeperActx.xml", "testDempsy/ClusterManager-LocalVmActx.xml" };
   String[] transports = new String[]
         { "testDempsy/Transport-PassthroughActx.xml", 
         "testDempsy/Transport-BlockingQueueActx.xml", 
         null // this means use the alternatingTransports
         };
   
   String[] alternatingTransports = { "testDempsy/Transport-TcpActx.xml", "testDempsy/Transport-TcpWithOverflowActx.xml" };

   // bad combinations.
   List<ClusterId> badCombos = Arrays.asList(new ClusterId[] {
         // this is a hack ... use a ClusterId as a String tuple for comparison
         
         // the passthrough Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterManager-ZookeeperActx.xml", "testDempsy/Transport-PassthroughActx.xml") , 
         
         // the blockingqueue Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterManager-ZookeeperActx.xml", "testDempsy/Transport-BlockingQueueActx.xml") 
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
   
   public static class KeyStoreImpl implements KeyStore<String>
   {
      @Override
      public Iterable<String> getAllPossibleKeys()
      {
         ArrayList<String> keys = new ArrayList<String>();
         keys.add("test1");
         keys.add("test2");
         return keys;
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
         for (String transport : transports)
         {
            // select one of the alternatingTransports
            if (transport == null)
               transport = alternatingTransports[runCount % alternatingTransports.length];

            // alternate the dempsy configs
            String dempsyConfig = dempsyConfigs[runCount % dempsyConfigs.length];

            if (! badCombos.contains(new ClusterId(clusterManager,transport)))
            {
               try
               {
                  logger.debug("*****************************************************************");
                  logger.debug(" test: " + (checker == null ? "none" : checker) + " using " + dempsyConfig + "," + clusterManager + "," + transport);
                  logger.debug("*****************************************************************");

                  String[] ctx = new String[4];
                  ctx[0] = dempsyConfig;
                  ctx[1] = clusterManager;
                  ctx[2] = transport;
                  ctx[3] = "testDempsy/" + applicationContext;

                  logger.debug("Starting up the appliction context ...");
                  ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(ctx);
                  actx.registerShutdownHook();

                  Dempsy dempsy = (Dempsy)actx.getBean("dempsy");

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
                  logger.error("***************** FAILED ON: " + clusterManager + ", " + transport);
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
            "testDempsy/ClusterManager-LocalVmActx.xml",
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
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-InValidClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterManager-LocalVmActx.xml",
            "testDempsy/SimpleMultistageApplicationActx.xml"
            );
      actx.registerShutdownHook();
      actx.stop();
      actx.destroy();
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
               "testDempsy/ClusterManager-LocalVmActx.xml",
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
                  for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && !message.equals(mp.lastReceived.get());)
                     Thread.sleep(1);
                  assertEquals(message,mp.lastReceived.get());

                      
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
                  for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && !message.equals(mp.lastReceived.get());)
                     Thread.sleep(1);
                  assertEquals(message,mp.lastReceived.get());
                  
                  assertEquals(adaptor2.lastSent,message);
                  assertEquals(adaptor2.lastSent,mp.lastReceived.get());
                  
               }
               
               public String toString() { return "testMessageThrough"; }
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
                  for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && mp.cloneCalls.get()<2;)
                     Thread.sleep(1);
                  
                  assertEquals(2, mp.cloneCalls.get());

                  TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  adaptor.pushMessage(new TestMessage("output")); // this causes the container to clone the Mp

                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && mp.cloneCalls.get()<3;)
                     Thread.sleep(1);
                  
                  assertEquals(3, mp.cloneCalls.get());
                  
                  adaptor.pushMessage(new TestMessage("test1")); // this causes the container to clone the Mp

                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && mp.cloneCalls.get()<3;)
                     Thread.sleep(1);
                  assertEquals(3, mp.cloneCalls.get());
                  List<Node> nodes = dempsy.getCluster(new ClusterId("test-app","test-cluster1")).getNodes();
                  Assert.assertNotNull(nodes);
                  Assert.assertTrue(nodes.size()>0);
                  Node node = nodes.get(0);
                  Assert.assertNotNull(node);
                  double duration = node.getStatsCollector().getPreInstantiationDuration();
                  Assert.assertTrue(duration>0.0);
               }
               
               public String toString() { return "testMPStartMethod"; }
            });
   }   


}
