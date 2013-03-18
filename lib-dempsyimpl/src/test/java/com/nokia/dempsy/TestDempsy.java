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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.messagetransport.util.ForwardedReceiver;

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
            ForwardedReceiver r = (ForwardedReceiver)cluster.getNodes().get(0).receiver;
            executor = (DefaultDempsyExecutor)r.getExecutor();
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
               defaultClusterCheck,
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
      runAllCombinations(new Checker()
      {
         @Override
         public void check(ClassPathXmlApplicationContext context) throws Throwable { }
         
         public String toString() { return "testStartupShutdown"; }

      },"SimpleMultistageApplicationActx.xml");

   }
   
   @Test
   public void testMpStartMethod() throws Throwable
   {
      runAllCombinations(new Checker()   
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable
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
            }, "SinglestageApplicationActx.xml");
   }
   
   @Test
   public void testMessageThrough() throws Throwable
   {
      runAllCombinations(new Checker()
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable
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
            }, "SinglestageApplicationActx.xml");
   }
   
   @Test
   public void testMessageThroughWithClusterFailure() throws Throwable
   {
      runAllCombinations(new Checker()
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable
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
                           try
                           {
                              adaptor.pushMessage(new TestMessage("Hello:" + count++));
                              try { Thread.sleep(10); } catch (Throwable th) {}
                           }
                           catch (RuntimeException th)
                           {
                              System.err.println("Failed in adaptor send. Test is likely to fail.");
                              th.printStackTrace(System.err);
                              throw th;
                           }
                        }
                     }
                  });
                  
                  thread.setDaemon(true);
                  thread.start();
                  
                  try
                  {
                     for (int i = 0; i < 10; i++)
                     {
                        logger.trace("=========================");
                        dsess.disrupt();

                        // now wait until more messages come through
                        final long curCount = mp.handleCalls.get();
                        assertTrue("Failed to increment calls on the " + i + "'th/nd/st try",
                              poll(baseTimeoutMillis,mp, new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.handleCalls.get() > curCount; } }));
                     }
                  }
                  finally
                  {
                     stopSending.set(true);
                     thread.interrupt();
                  }
               }
               
               public String toString() { return "testMessageThroughWithClusterFailure"; }
            },"SinglestageApplicationActx.xml");
   }

   
   @Test
   public void testOutPutMessage() throws Throwable
   {
      runAllCombinations(new Checker()
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable
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

            }, "SinglestageOutputApplicationActx.xml");
   }


   @Test
   public void testCronOutPutMessage() throws Throwable
   {
      // since the cron output message can only go to 1 second resolution,
      //  we need to drop the number of attempt to 3. Otherwise this test
      //  takes way too long.
      TestMp.currentOutputCount = 3;
      runAllCombinations(new Checker()
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable
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

            },"SinglestageOutputApplicationActx.xml");
   }

   @Test
   public void testExplicitDesintationsStartup() throws Throwable
   {
      runAllCombinations(new Checker()
            {
               @Override
               public void check(ClassPathXmlApplicationContext context) throws Throwable { }
               
               public String toString() { return "testExplicitDesintationsStartup"; }

            },"MultistageApplicationExplicitDestinationsActx.xml");
   }
   
}
