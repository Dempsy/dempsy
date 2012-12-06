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

import static com.nokia.dempsy.TestUtils.getAdaptor;
import static com.nokia.dempsy.TestUtils.getMp;
import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.messagetransport.tcp.TcpReceiver;
import com.nokia.dempsy.messagetransport.tcp.TcpReceiverAccess;
import com.nokia.dempsy.monitoring.coda.MetricGetters;

public class TestDempsy extends DempsyTestBase
{
   static
   {
      logger = LoggerFactory.getLogger(TestDempsy.class);
   }
   
   @Test 
   public void testIndividualClusterStart() throws Throwable
   {
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-IndividualClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml",
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
            "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml",
            "testDempsy/SimpleMultistageApplicationActx.xml"
            );
   }
   
   @Test
   public void testTcpTransportExecutorConfigurationThroughApplication() throws Throwable
   {
      ClassPathXmlApplicationContext actx = null;
      DefaultDempsyExecutor executor = null;
      try
      {
         actx = new ClassPathXmlApplicationContext(
               "testDempsy/Dempsy-IndividualClusterStart.xml",
               "testDempsy/Transport-TcpActx.xml",
               "testDempsy/ClusterInfo-LocalActx.xml",
               "testDempsy/Serializer-KryoActx.xml",
               "testDempsy/RoutingStrategy-DecentralizedActx.xml",
               "testDempsy/SimpleMultistageApplicationWithExecutorActx.xml"
               );
         actx.registerShutdownHook();

         Dempsy dempsy = (Dempsy)actx.getBean("dempsy");
         for (Dempsy.Application.Cluster cluster : dempsy.applications.values().iterator().next().appClusters)
         {
            // get the receiver from the node
            TcpReceiver r = (TcpReceiver)cluster.getNodes().get(0).receiver;
            executor = (DefaultDempsyExecutor)TcpReceiverAccess.getExecutor(r);
            assertEquals(123456,executor.getMaxNumberOfQueuedLimitedTasks());
            assertTrue(executor.isRunning());
         }
      }
      finally
      {
         try { actx.stop(); } catch (Throwable th) {}
         try { actx.destroy(); } catch(Throwable th) {}
      }
      
      assertNotNull(executor);
      assertTrue(!executor.isRunning());
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
               "testDempsy/Serializer-KryoActx.xml",
               "testDempsy/RoutingStrategy-DecentralizedActx.xml",
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
      runAllCombinations("SimpleMultistageApplicationActx.xml", new Checker()
      {
         @Override
         public void check(ApplicationContext context) throws Throwable { }
         
         public String toString() { return "testStartupShutdown"; }

      });

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
   public void testMessageThroughWithClusterFailure() throws Throwable
   {
      runAllCombinations("SinglestageApplicationActx.xml",
            new Checker()
            {
               @Override
               public void check(ApplicationContext context) throws Throwable
               {
                  // check that the message didn't go through.
                  Dempsy dempsy = (Dempsy)context.getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");

                  final AtomicReference<TestMessage> msg = new AtomicReference<TestMessage>();
                  final TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
                  // now send a message through
                  
                  TestMessage message = new TestMessage("HereIAm - testMessageThrough");
                  adaptor.pushMessage(message);
                  
                  // instead of the latch we are going to poll for the correct result
                  // wait for it to be received.
                  msg.set(message);
                  assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return msg.get().equals(mp.lastReceived.get()); } }));
                  
                  assertEquals(adaptor.lastSent,message);
                  assertEquals(adaptor.lastSent,mp.lastReceived.get());
                  
                  // now go into a disruption loop
                  ClusterInfoSession session = TestUtils.getSession(dempsy.getCluster(new ClusterId("test-app","test-cluster1")));
                  assertNotNull(session);
                  DisruptibleSession dsess = (DisruptibleSession) session;
                  
                  final AtomicBoolean stopSending = new AtomicBoolean(false);
                  
                  Thread thread = new Thread(new Runnable()
                  {
                     
                     @Override
                     public void run()
                     {
                        long count = 0;
                        while (!stopSending.get())
                        {
                           adaptor.pushMessage(new TestMessage("Hello:" + count++));
                           try { Thread.sleep(1); } catch (Throwable th) {}
                        }
                     }
                  });
                  
                  thread.setDaemon(true);
                  thread.start();
                  
                  for (int i = 0; i < 10; i++)
                  {
                     logger.trace("=========================");
                     dsess.disrupt();
                     
                     // now wait until more messages come through
                     final long curCount = mp.handleCalls.get();
                     assertTrue(poll(baseTimeoutMillis,mp, new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get() > curCount; } }));
                  }
                  
                  stopSending.set(true);
               }
               
               public String toString() { return "testMessageThroughWithClusterFailure"; }
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
      // since the cron output message can only go to 1 second resolution,
      //  we need to drop the number of attempt to 3. Otherwise this test
      //  takes way too long.
      TestMp.currentOutputCount = 3;
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
                  assertTrue(mp.outputCount.get()>=3);
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
               public void check(ApplicationContext context) throws Throwable { }
               
               public String toString() { return "testExplicitDesintationsStartup"; }

            });
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
      TestMp.alwaysPauseOnActivation = true;
      runMpKeyStoreTest("testMpKeyStoreWithFailingClusterManager");
   }
   
   public void runMpKeyStoreTest(final String methodName) throws Throwable
   {
      Checker checker = new Checker()   
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

            // The KeySourceImpl ought to create 2 Mps (given infinite is 'false'). Wait for them
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==2; } }));
            
            TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
            adaptor.pushMessage(new TestMessage("output")); // this causes the container to clone a third Mp

            // Wait for that third to be created
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==3; } }));
            
            // This ought to result in a message sent to a preinstantiation Mp and
            //  so no increment in the clone count.
            adaptor.pushMessage(new TestMessage("test1")); // this causes the container to clone the Mp

            Thread.sleep(100); // give it time to work it's way through
            assertEquals(3, mp.cloneCalls.get()); // check that it didn't cause a clone.
            
            // make sure that the PreInstantiation ran.
            List<Node> nodes = dempsy.getCluster(new ClusterId("test-app","test-cluster1")).getNodes();
            assertNotNull(nodes);
            assertTrue(nodes.size()>0);
            Node node = nodes.get(0);
            assertNotNull(node);
            assertTrue(((MetricGetters)node.getStatsCollector()).getPreInstantiationCount() > 0);
         }
         
         public String toString() { return methodName; }
      };

      runAllCombinations("SinglestageWithKeyStoreApplicationActx.xml", checker);
      runAllCombinations("SinglestageWithKeyStoreAndExecutorApplicationActx.xml", checker);
   }
   
   @Test
   public void testOverlappingKeyStoreCalls() throws Throwable
   {
      KeySourceImpl.pause = new CountDownLatch(1);
      KeySourceImpl.infinite = true;

      Checker checker = new Checker()   
      {
         @Override
         public void setup()
         {
            KeySourceImpl.pause = new CountDownLatch(1);
            KeySourceImpl.infinite = true;
            KeySourceImpl.lastCreated = null;
         }
         
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
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get() > 1000; } }));

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
         }
         
         public String toString() { return "testOverlappingKeyStoreCalls"; }
      };
      
      runAllCombinations("SinglestageWithKeyStoreApplicationActx.xml",checker);
      runAllCombinations("SinglestageWithKeyStoreAndExecutorApplicationActx.xml",checker);
   }
   
   @Test
   public void testFailedMessageHandlingWithKeyStore() throws Throwable
   {
      Checker checker = new Checker()   
      {
         @Override
         public void setup() { KeySourceImpl.pause = new CountDownLatch(1); }

         @Override
         public void check(ApplicationContext context) throws Throwable
         {
            // start things and verify that the init method was called
            Dempsy dempsy = (Dempsy)context.getBean("dempsy");
            TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                
            // verify we haven't called it again, not that there's really
            // a way to given the code
            assertEquals(1, mp.startCalls.get());
            
            // make sure that there are no Mps
            MetricGetters statsCollector = (MetricGetters)dempsy.getCluster(new ClusterId("test-app","test-cluster1")).getNodes().get(0).getStatsCollector();
            Thread.sleep(10);
            assertEquals(0,statsCollector.getMessageProcessorsCreated());
            
            mp.failActivation.set("test1");
            TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
            adaptor.pushMessage(new TestMessage("test1")); // this causes the container to clone the Mp

            Thread.sleep(100);
            assertEquals(0,statsCollector.getMessageProcessorsCreated());
            
            mp.failActivation.set(null);
            KeySourceImpl.pause.countDown();

            // Wait for the 3 clone calls expected because of 1 failure plus
            // a preinstantiation of 2 MPs.
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==3; } }));
            
            assertTrue(poll(baseTimeoutMillis,statsCollector,new Condition<MetricGetters>() { @Override public boolean conditionMet(MetricGetters mg) {  return mg.getMessageProcessorsCreated()==2; } }));
            adaptor.pushMessage(new TestMessage("test1"));
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==1; } }));
            adaptor.pushMessage(new TestMessage("test2"));
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==2; } }));
            adaptor.pushMessage(new TestMessage("test1"));
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==3; } }));
            adaptor.pushMessage(new TestMessage("test2"));
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==4; } }));
            adaptor.pushMessage(new TestMessage("test1"));
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==5; } }));
            adaptor.pushMessage(new TestMessage("test2"));

            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get()==6; } }));
            Thread.sleep(100);
            assertEquals(6,mp.handleCalls.get());
            assertEquals(3,mp.cloneCalls.get());
            assertEquals(2,statsCollector.getMessageProcessorsCreated());
            
         }
         
         public String toString() { return "testFailedMessageHandlingWithKeyStore"; }
      };

      // make sure both exceptions are handled since the logic in the container
      // actually varies depending on whether or not the exception is checked or not.
      TestMp.activateCheckedException = true;
      runAllCombinations("SinglestageWithKeyStoreApplicationActx.xml",checker);
      TestMp.activateCheckedException = false;
      runAllCombinations("SinglestageWithKeyStoreAndExecutorApplicationActx.xml",checker);
   }

   @Test
   public void testDynamicTopologyConfig()
   {
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-IndividualClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml",
            "testDempsy/DTSimpleMultistageApplicationActx.xml"
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
   
   @Test
   public void testMultiNodeDempsy() throws Throwable
   {
      final int nodeCount = 5;
      System.setProperty("nodecount", "" + nodeCount);
      TestZookeeperSessionFactory.useSingletonSession = true;
      
      String[][] acts = new String[nodeCount][];
      for (int i = 0; i < nodeCount; i++)
         acts[i] = new String[]{ "SimpleMultistageApplicationWithExecutorActx.xml" };
               
      runAllCombinationsMultiDempsy(new MultiCheck()
      {
         @Override
         public void check(ApplicationContext[] actx) throws Throwable
         {
            // each run we need to reset the globalHandleCalls
            TestMp.globalHandleCalls.set(0);

            TestAdaptor adaptor = actx[0].getBean(TestAdaptor.class);
            adaptor.dispatcher.dispatch(new TestMessage("Hello"));

            // send a message through ... it should go to an Mp in all three clusters.
            assertTrue(poll(baseTimeoutMillis,null,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return TestMp.globalHandleCalls.get()==3; } }));

            Thread.sleep(100);
            assertEquals(3,TestMp.globalHandleCalls.get());
         }
         
         @Override
         public String toString() { return "testMultiNodeDempsy"; }
         
      },acts);
      

   }

}
