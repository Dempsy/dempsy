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

import static com.nokia.dempsy.TestUtils.getMp;
import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy.Application.Cluster.Node;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.monitoring.coda.MetricGetters;

public class TestDempsyKeySource extends DempsyTestBase
{
   static
   {
      logger = LoggerFactory.getLogger(TestDempsyKeySource.class);
   }
   
   @Before
   public void init()
   {
      KeySourceImpl.disruptSession = false;
      KeySourceImpl.infinite = false;
      KeySourceImpl.pause = new CountDownLatch(0);
      super.init(); // Apparently @Before isn't inherited.
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

   @Test
   public void testMpKeySource() throws Throwable
   {
      runMpKeySourceTest("testMpKeySource");
   }
   
   @Test
   public void testMpKeySourceWithFailingClusterManager() throws Throwable
   {
      KeySourceImpl.disruptSession = true;
      TestMp.alwaysPauseOnActivation = true;
      runMpKeySourceTest("testMpKeySourceWithFailingClusterManager");
   }
   
   public void runMpKeySourceTest(final String methodName) throws Throwable
   {
      Checker checker = new Checker()   
      {
         @Override
         public void check(ClassPathXmlApplicationContext context) throws Throwable
         {
            // start things and verify that the init method was called
            Dempsy dempsy = (Dempsy)context.getBean("dempsy");
            TestMp mp = (TestMp) getMp(dempsy, "test-app","test-cluster1");
                
            // verify we haven't called it again, not that there's really
            // a way to given the code
            assertEquals(1, mp.startCalls.get());

            // The KeySourceImpl ought to create 2 Mps (given infinite is 'false'). Wait for them
            assertTrue(poll(baseTimeoutMillis,mp,new Condition<TestMp>() { @Override public boolean conditionMet(TestMp mp) {  return mp.cloneCalls.get()==2; } }));

            // now make sure the Inbound's have been reset ... in the case of a disruption the send will 
            // fail until the session is back and the Inbound's are reset.
            TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy);
            
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

      runAllCombinations(checker, "SinglestageWithKeySourceApplicationActx.xml");
      runAllCombinations(checker, "SinglestageWithKeySourceAndExecutorApplicationActx.xml");
   }
   
   @Test
   public void testOverlappingKeySourceCalls() throws Throwable
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
         public void check(ClassPathXmlApplicationContext context) throws Throwable
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
            container.keyspaceResponsibilityChanged(false, true);
            
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
         
         public String toString() { return "testOverlappingKeySourceCalls"; }
      };
      
      runAllCombinations(checker, "SinglestageWithKeySourceApplicationActx.xml");
      runAllCombinations(checker, "SinglestageWithKeySourceAndExecutorApplicationActx.xml");
   }
   
   @Test
   public void testFailedMessageHandlingWithKeySource() throws Throwable
   {
      Checker checker = new Checker()   
      {
         @Override
         public void setup() { KeySourceImpl.pause = new CountDownLatch(1); }

         @Override
         public void check(ClassPathXmlApplicationContext context) throws Throwable
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
            
            mp.failASingleActivationForThisKey.set("test1");
            TestAdaptor adaptor = (TestAdaptor)context.getBean("adaptor");
            adaptor.pushMessage(new TestMessage("test1")); // this causes the container to attempt clone the Mp
                                                           //  but it fails in deference to the pre-instantiation

            Thread.sleep(100);
            assertEquals(0,statsCollector.getMessageProcessorsCreated());
            
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
         
         public String toString() { return "testFailedMessageHandlingWithKeySource"; }
      };

      // make sure both exceptions are handled since the logic in the container
      // actually varies depending on whether or not the exception is checked or not.
      TestMp.activateCheckedException = true;
      runAllCombinations(checker,"SinglestageWithKeySourceApplicationActx.xml");
      TestMp.activateCheckedException = false;
      runAllCombinations(checker,"SinglestageWithKeySourceAndExecutorApplicationActx.xml");
   }
}
