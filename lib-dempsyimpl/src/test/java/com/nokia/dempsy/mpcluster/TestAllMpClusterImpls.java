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

package com.nokia.dempsy.mpcluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;

public class TestAllMpClusterImpls
{
   String[] clusterManagers = new String[]{ "testDempsy/ClusterManager-ZookeeperActx.xml", "testDempsy/ClusterManager-LocalVmActx.xml" };
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterManager-ZookeeperActx.xml" };
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterManager-LocalVmActx.xml" };
   
   @SuppressWarnings("rawtypes")
   private List<MpClusterSession> sessionsToClose = new ArrayList<MpClusterSession>();

   public void cleanup()
   {
      for(@SuppressWarnings("rawtypes") MpClusterSession session : sessionsToClose)
         session.stop();
      sessionsToClose.clear();
   }
   
   private static InitZookeeperServerBean zkServer = null;

   @BeforeClass
   public static void setupZookeeperSystemVars() throws IOException
   {
      zkServer = new InitZookeeperServerBean();
   }
   
   @AfterClass
   public static void shutdownZookeeper()
   {
      zkServer.stop();
   }
   
   private static interface Checker<T,N>
   {
      public void check(String pass, MpClusterSessionFactory<T,N> factory) throws Throwable;
   }
   
   private <T,N> void runAllCombinations(Checker<T,N> checker) throws Throwable
   {
      for (String clusterManager : clusterManagers)
      {
         ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(clusterManager);
         actx.registerShutdownHook();

         @SuppressWarnings("unchecked")
         MpClusterSessionFactory<T, N> factory = (MpClusterSessionFactory<T, N>)actx.getBean("clusterSessionFactory");

         if (checker != null)
            checker.check("pass for:" + clusterManager,factory);

         actx.stop();
         actx.destroy();
      }
   }
   
   private interface Condition<T>
   {
      public boolean conditionMet(T o);
   }

   public static <T> boolean poll(long timeoutMillis, T userObject, Condition<T> condition) throws InterruptedException
   {
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !condition.conditionMet(userObject);)
         Thread.sleep(1);
      return condition.conditionMet(userObject);
   }
   
   @Test
   public void testMpClusterFromFactory() throws Throwable
   {
      runAllCombinations(new Checker<String,String>()
      {
         @Override
         public void check(String pass, MpClusterSessionFactory<String,String> factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app1","test-cluster");
            
            MpClusterSession<String, String> session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            MpCluster<String, String> clusterHandle = session.getCluster(cid);
            assertNotNull(pass,clusterHandle);
            
            // there should be nothing currently registered
            Collection<MpClusterSlot<String>> slots = clusterHandle.getActiveSlots();
            assertNotNull(pass,slots);
            assertEquals(pass,0,slots.size());
            
            assertNull(pass,clusterHandle.getClusterData());
            
            session.stop();
         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelData() throws Throwable
   {
      runAllCombinations(new Checker<String,String>()
      {
         @Override
         public void check(String pass, MpClusterSessionFactory<String,String> factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app2","test-cluster");
            
            MpClusterSession<String, String> session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            MpCluster<String, String> clusterHandle = session.getCluster(cid);
            assertNotNull(pass,clusterHandle);

            String data = "HelloThere";
            clusterHandle.setClusterData(data);
            String cdata = clusterHandle.getClusterData();
            assertEquals(pass,data,cdata);
            
            session.stop();

         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelDataThroughApplication() throws Throwable
   {
      runAllCombinations(new Checker<String,String>()
      {
         @Override
         public void check(String pass, MpClusterSessionFactory<String,String> factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app3","testSimpleClusterLevelDataThroughApplication");
            
            MpClusterSession<String, String> session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            MpApplication<String, String> mpapp = session.getApplication(cid.getApplicationName());
            MpCluster<String, String> clusterHandle = session.getCluster(cid);
            assertNotNull(pass,clusterHandle);
            Collection<MpCluster<String, String>> clusterHandles = mpapp.getActiveClusters();
            assertNotNull(pass,clusterHandles);
            assertEquals(1,clusterHandles.size());
            assertEquals(clusterHandle,clusterHandles.iterator().next());

            String data = "HelloThere";
            clusterHandle.setClusterData(data);
            String cdata = clusterHandle.getClusterData();
            assertEquals(pass,data,cdata);
            
            session.stop();

         }
         
      });
   }

   
   @Test
   public void testSimpleJoinTest() throws Throwable
   {
      runAllCombinations(new Checker<String, String>()
      {
         @Override
         public void check(String pass, MpClusterSessionFactory<String, String> factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app4","test-cluster");

            MpClusterSession<String, String> session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            MpCluster<String, String> cluster = session.getCluster(cid);
            assertNotNull(pass,cluster);

            MpClusterSlot<String> node = cluster.join("Test");
            Assert.assertEquals(1, cluster.getActiveSlots().size());

            String data = "testSimpleJoinTest-data";
            node.setSlotInformation(data);
            Assert.assertEquals(pass,data, node.getSlotInformation());
            
            node.leave();
            
            // wait for no more than ten seconds
            for (long timeToEnd = System.currentTimeMillis() + 10000; timeToEnd > System.currentTimeMillis();)
            {
               Thread.sleep(10);
               if (cluster.getActiveSlots().size() == 0)
                  break;
            }
            Assert.assertEquals(pass,0, cluster.getActiveSlots().size());
            
            session.stop();
         }
      });
   }
   
   private class TestWatcher implements MpClusterWatcher
   {
      public boolean recdUpdate = false;
      public CountDownLatch latch;
      
      public TestWatcher(int count) { latch = new CountDownLatch(count); }
      
      @Override
      public void process() 
      {
         System.out.println("Hello");
         recdUpdate = true;
         latch.countDown();
      }
       
   }
   
   @Test
   public void testSimpleWatcherData() throws Throwable
   {
      runAllCombinations(new Checker<String,String>()
      {
         @Override
         public void check(String pass, MpClusterSessionFactory<String,String> factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app5","test-cluster");
            
            MpClusterSession<String, String> mainSession = factory.createSession();
            assertNotNull(pass,mainSession);            
            sessionsToClose.add(mainSession);
            
            TestWatcher mainAppWatcher = new TestWatcher(1);
            MpApplication<String, String> mpapp = mainSession.getApplication(cid.getApplicationName());
            mpapp.addWatcher(mainAppWatcher);
            assertEquals(0,mpapp.getActiveClusters().size());
            
            MpClusterSession<String, String> otherSession = factory.createSession();
            assertNotNull(pass,otherSession);
            sessionsToClose.add(otherSession);
            
            assertFalse(pass,mainSession.equals(otherSession));
            
            MpCluster<String, String> clusterHandle = mainSession.getCluster(cid);
            assertNotNull(pass,clusterHandle);
            
            assertTrue(poll(5000, mainAppWatcher, new Condition<TestWatcher>() {
               public boolean conditionMet(TestWatcher o) { return o.recdUpdate;  }
            }));
            
            mainAppWatcher.recdUpdate = false;

            MpCluster<String,String> otherCluster = otherSession.getCluster(cid);
            assertNotNull(pass,otherCluster);

            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);

            TestWatcher mainWatcher = new TestWatcher(1);
            clusterHandle.addWatcher(mainWatcher);
            
            TestWatcher otherWatcher = new TestWatcher(1);
            otherCluster.addWatcher(otherWatcher);
            
            String data = "HelloThere";
            clusterHandle.setClusterData(data);
            
            // this should have affected otherWatcher 
            assertTrue(pass,otherWatcher.latch.await(5, TimeUnit.SECONDS));
            assertTrue(pass,otherWatcher.recdUpdate);
            
            // we do expect an update here also
            assertTrue(pass,mainWatcher.latch.await(5,TimeUnit.SECONDS));
            assertTrue(pass,mainWatcher.recdUpdate);
            
            // now check access through both sessions and we should see the update.
            String cdata = clusterHandle.getClusterData();
            assertEquals(pass,data,cdata);
            
            cdata = otherCluster.getClusterData();
            assertEquals(pass,data,cdata);
            
            mainSession.stop();
            otherSession.stop();
            
            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);
         }
         
      });
   }
   
   private MpClusterSession<String, String> session1;
   private MpClusterSession<String, String> session2;
   private volatile boolean thread1Passed = false;
   private volatile boolean thread2Passed = false;
   private CountDownLatch latch = new CountDownLatch(1);

   @Test
   public void testConsumerCluster() throws Throwable
   {
      System.out.println("Testing Consumer Cluster");
      
      runAllCombinations(new Checker<String,String>()
      {
         @Override
         public void check(String pass,MpClusterSessionFactory<String,String> factory) throws Throwable
         {
            session1 = factory.createSession();
            sessionsToClose.add(session1);
            session2 = factory.createSession();
            sessionsToClose.add(session2);
            
            Thread t1 = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  ClusterId cid = new ClusterId("test-app6","test-cluster");
                  System.out.println("Consumer setting data");
                  try
                  {
                     MpCluster<String, String> consumer = session1.getCluster(cid);
                     consumer.setClusterData("Test");
                     thread1Passed = true;
                     latch.countDown();
                  }
                  catch(Exception e)
                  {
                     e.printStackTrace();
                  }

               }
            });
            t1.start();
            Thread t2 = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  ClusterId cid = new ClusterId("test-app6","test-cluster");
                  try
                  {
                     latch.await(10, TimeUnit.SECONDS);
                     MpCluster<String, String> producer = session2.getCluster(cid);
                     
                     String data = producer.getClusterData();
                     System.out.println(data);
                     if ("Test".equals(data))
                        thread2Passed = true;
                  }
                  catch(Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            });
            t2.start();

            t1.join(30000);
            t2.join(30000); // use a timeout just in case. A failure should be indicated below if the thread never finishes.

            assertTrue(pass,thread1Passed);
            assertTrue(pass,thread2Passed);
            
            session2.stop();
            session1.stop();
         }
      });
   }
}
