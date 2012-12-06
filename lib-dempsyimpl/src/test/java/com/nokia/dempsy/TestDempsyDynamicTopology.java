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

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.config.ClusterId;

public class TestDempsyDynamicTopology extends DempsyTestBase
{
   static
   {
      logger = LoggerFactory.getLogger(TestDempsyDynamicTopology.class);
   }
   
   @Test
   public void testDynamicTopologyConfigWholeApp()
   {
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-IndividualClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml",
            "testDempsy/SimpleMultistageApplication/appdef.xml","testDempsy/SimpleMultistageApplication/cluster0.xml",
            "testDempsy/SimpleMultistageApplication/cluster1.xml","testDempsy/SimpleMultistageApplication/cluster2.xml",
            "testDempsy/SimpleMultistageApplication/cluster3.xml"
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
   public void testDynamicTopologyConfigSingleCluster()
   {
      ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy-IndividualClusterStart.xml",
            "testDempsy/Transport-PassthroughActx.xml",
            "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml",
            "testDempsy/SimpleMultistageApplication/appdef.xml","testDempsy/SimpleMultistageApplication/cluster2.xml"
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
   public void testMessageThrough() throws Throwable
   {
      runAllCombinations(
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
            }, "SinglestageApplication/appdef.xml","SinglestageApplication/cluster0.xml","SinglestageApplication/cluster1.xml");
   }
   
   @Test
   public void testBadConfig() throws Throwable
   {
      boolean gotCorrectError = false;
      try
      {
         new ClassPathXmlApplicationContext(
               "testDempsy/Dempsy-IndividualClusterStart.xml",
               "testDempsy/Transport-PassthroughActx.xml",
               "testDempsy/ClusterInfo-LocalActx.xml",
               "testDempsy/Serializer-KryoActx.xml",
               "testDempsy/RoutingStrategy-DecentralizedActx.xml",
               "testDempsy/SinglestageApplication/appdef.xml","testDempsy/SinglestageApplication/bad-cluster0.xml",
               "testDempsy/SinglestageApplication/cluster1.xml"
               );
      }
      catch (BeanCreationException e)
      {
         if (e.getCause() instanceof DempsyException)
            gotCorrectError = true;
      }
      assertTrue(gotCorrectError);
   }

   @Test
   public void testMessageStartMultiDempsy() throws Throwable
   {
      String[][] ctxs = {
            { "SinglestageApplication/appdef.xml","SinglestageApplication/cluster0.xml" },
            { "SinglestageApplication/appdef.xml","SinglestageApplication/cluster1.xml" } 
      };
      
      runAllCombinationsMultiDempsy(null, ctxs );
   }

   @Test
   public void testMessageThroughMultiDempsy() throws Throwable
   {
      String[][] ctxs = {
            { "SinglestageApplication/appdef.xml","SinglestageApplication/cluster0.xml" },
            { "SinglestageApplication/appdef.xml","SinglestageApplication/cluster1.xml" } 
      };
      runAllCombinationsMultiDempsy(
            new MultiCheck()
            {
               @Override
               public void check(ApplicationContext[] context) throws Throwable
               {
                  Dempsy dempsy0 = (Dempsy)context[0].getBean("dempsy");
                  TestAdaptor adaptor = (TestAdaptor)context[0].getBean("adaptor");
                  Object message = new Object();
                  adaptor.pushMessage(message);
                  
                  // check that the message didn't go through.
                  Dempsy dempsy1 = (Dempsy)context[1].getBean("dempsy");
                  TestMp mp = (TestMp) getMp(dempsy1, "test-app","test-cluster1");
                  assertTrue(mp.lastReceived.get() == null);
                  
                  TestAdaptor adaptor2 = (TestAdaptor)getAdaptor(dempsy0, "test-app","test-cluster0");
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
            }, ctxs);
   }
   

}
