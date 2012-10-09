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

package com.nokia.dempsy.cluster;

import static com.nokia.dempsy.TestUtils.createApplicationLevel;
import static com.nokia.dempsy.TestUtils.createClusterLevel;
import static com.nokia.dempsy.TestUtils.poll;
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;
import com.nokia.dempsy.config.ClusterId;

public class TestAllClusterImpls
{
   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-LocalActx.xml", "testDempsy/ClusterInfo-ZookeeperActx.xml" };
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-LocalActx.xml" };
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-ZookeeperActx.xml" };
   
   private List<ClusterInfoSession> sessionsToClose = new ArrayList<ClusterInfoSession>();

   public void cleanup()
   {
      for(ClusterInfoSession session : sessionsToClose)
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
   
   private static interface Checker
   {
      public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable;
   }
   
   private <T,N> void runAllCombinations(Checker checker) throws Throwable
   {
      for (String clusterManager : clusterManagers)
      {
         ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(clusterManager);
         actx.registerShutdownHook();

         ClusterInfoSessionFactory factory = (ClusterInfoSessionFactory)actx.getBean("clusterSessionFactory");

         if (checker != null)
            checker.check("pass for:" + clusterManager,factory);

         actx.stop();
         actx.destroy();
      }
   }
      
   private static String getClusterLeaf(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      return session.exists(cid.asPath(), null) ? cid.asPath() : null;
   }
   
   @Test
   public void testMpClusterFromFactory() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app1","test-cluster");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String clusterPath = createClusterLevel(cid, session);
            
            assertEquals(clusterPath,cid.asPath());
            
            assertNotNull(pass,clusterPath);
            assertTrue(pass,session.exists(clusterPath, null));
            
            // there should be nothing currently registered
            Collection<String> slots = session.getSubdirs(clusterPath,null);
            assertNotNull(pass,slots);
            assertEquals(pass,0,slots.size());
            
            assertNull(pass,session.getData(clusterPath,null));
            
            session.stop();
         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelData() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app2","test-cluster");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String clusterPath = createClusterLevel(cid,session);
            assertNotNull(pass,clusterPath);

            String data = "HelloThere";
            session.setData(clusterPath,data);
            String cdata = (String)session.getData(clusterPath,null);
            assertEquals(pass,data,cdata);
            
            session.stop();
         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelDataThroughApplication() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app3","testSimpleClusterLevelDataThroughApplication");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String mpapp = createApplicationLevel(cid,session);
            String clusterPath = mpapp + "/" + cid.getMpClusterName();
            assertNotNull(pass,session.mkdir(clusterPath, DirMode.PERSISTENT));
            assertNotNull(pass,clusterPath);
            Collection<String> clusterPaths = session.getSubdirs(mpapp,null);
            assertNotNull(pass,clusterPaths);
            assertEquals(1,clusterPaths.size());
            assertEquals(cid.getMpClusterName(),clusterPaths.iterator().next());

            String data = "HelloThere";
            session.setData(clusterPath,data);
            String cdata = (String)session.getData(clusterPath,null);
            assertEquals(pass,data,cdata);
            
            session.stop();

         }
         
      });
   }

   @Test
   public void testSimpleJoinTest() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app4","test-cluster");

            final ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String cluster = createClusterLevel(cid,session);
            assertNotNull(pass,cluster);

            String node = cluster + "/Test";
            assertNotNull(session.mkdir(node,DirMode.EPHEMERAL));
            assertEquals(1, session.getSubdirs(cluster,null).size());

            String data = "testSimpleJoinTest-data";
            session.setData(node,data);
            assertEquals(pass,data, session.getData(node,null));
            
            session.rmdir(node);
            
            // wait for no more than ten seconds
            assertTrue(pass, poll(10000, cluster, new Condition<String>()
            {
               @Override
               public boolean conditionMet(String cluster) throws Throwable
               {
                  return session.getSubdirs(cluster, null).size() == 0;
               }
            })); 
            
            session.stop();
         }
      });
   }
   
   private class TestWatcher implements ClusterInfoWatcher
   {
      public boolean recdUpdate = false;
      public CountDownLatch latch;
      
      public TestWatcher(int count) { latch = new CountDownLatch(count); }
      
      @Override
      public void process() 
      {
         recdUpdate = true;
         latch.countDown();
      }
       
   }
   
   @Test
   public void testSimpleWatcherData() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app5","test-cluster");
            
            ClusterInfoSession mainSession = factory.createSession();
            assertNotNull(pass,mainSession);            
            sessionsToClose.add(mainSession);
            
            TestWatcher mainAppWatcher = new TestWatcher(1);
            String mpapp = createApplicationLevel(cid,mainSession);
            assertTrue(mainSession.exists(mpapp,null));
            mainSession.getSubdirs(mpapp, mainAppWatcher); // register mainAppWatcher for subdir
            assertEquals(0,mainSession.getSubdirs(mpapp,null).size());
            
            ClusterInfoSession otherSession = factory.createSession();
            assertNotNull(pass,otherSession);
            sessionsToClose.add(otherSession);
            
            assertFalse(pass,mainSession.equals(otherSession));
            
            String clusterHandle = mpapp + "/" + cid.getMpClusterName();
            mainSession.mkdir(clusterHandle,DirMode.PERSISTENT);
            assertTrue(pass,mainSession.exists(clusterHandle,null));
            
            assertTrue(poll(5000, mainAppWatcher, new Condition<TestWatcher>() {
               public boolean conditionMet(TestWatcher o) { return o.recdUpdate;  }
            }));
            
            mainAppWatcher.recdUpdate = false;

            String otherCluster = getClusterLeaf(cid,otherSession);
            assertNotNull(pass,otherCluster);
            assertEquals(pass,clusterHandle,otherCluster);

            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);

            TestWatcher mainWatcher = new TestWatcher(1);
            assertTrue(mainSession.exists(clusterHandle,mainWatcher));
            
            TestWatcher otherWatcher = new TestWatcher(1);
            assertTrue(otherSession.exists(otherCluster,otherWatcher));
            
            String data = "HelloThere";
            mainSession.setData(clusterHandle,data);
            
            // this should have affected otherWatcher 
            assertTrue(pass,otherWatcher.latch.await(5, TimeUnit.SECONDS));
            assertTrue(pass,otherWatcher.recdUpdate);
            
            // we do expect an update here also
            assertTrue(pass,mainWatcher.latch.await(5,TimeUnit.SECONDS));
            assertTrue(pass,mainWatcher.recdUpdate);
            
            // now check access through both sessions and we should see the update.
            String cdata = (String)mainSession.getData(clusterHandle,null);
            assertEquals(pass,data,cdata);
            
            cdata = (String)otherSession.getData(otherCluster,null);
            assertEquals(pass,data,cdata);
            
            mainSession.stop();
            otherSession.stop();
            
            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);
         }
         
      });
   }
   
   private ClusterInfoSession session1;
   private ClusterInfoSession session2;
   private volatile boolean thread1Passed = false;
   private volatile boolean thread2Passed = false;
   private CountDownLatch latch = new CountDownLatch(1);

   @Test
   public void testConsumerCluster() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass,ClusterInfoSessionFactory factory) throws Throwable
         {
            final ClusterId cid = new ClusterId("test-app6","test-cluster");
            session1 = factory.createSession();
            createClusterLevel(cid, session1);
            sessionsToClose.add(session1);
            session2 = factory.createSession();
            sessionsToClose.add(session2);
            
            Thread t1 = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  try
                  {
                     String consumer = getClusterLeaf(cid,session1);
                     session1.setData(consumer,"Test");
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
                  try
                  {
                     latch.await(10, TimeUnit.SECONDS);
                     String producer = getClusterLeaf(cid,session2);
                     
                     String data = (String)session2.getData(producer,null);
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
   
   @Test
   public void testGetSetDataNoNode() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app7","test-cluster");

            final ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String cluster = createClusterLevel(cid,session);
            assertNotNull(pass,cluster);

            String node = cluster + "/Test";
            String data = "testSimpleJoinTest-data";
            boolean gotExpectedException = false;
            try { session.setData(node,data); } catch (ClusterInfoException e){ gotExpectedException = true; }
            assertTrue(pass,gotExpectedException);
            
            gotExpectedException = false;
            try { session.rmdir(node); } catch (ClusterInfoException e){ gotExpectedException = true; }
            assertTrue(pass,gotExpectedException);
            
            gotExpectedException = false;
            try { session.getData(node,null); } catch (ClusterInfoException e){ gotExpectedException = true; }
            assertTrue(pass,gotExpectedException);
            
            session.stop();
         }
      });
   }
   
   @Test
   public void testNullWatcherBehavior() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            final ClusterId cid = new ClusterId("test-app2","testNullWatcherBehavior");
            final AtomicBoolean processCalled = new AtomicBoolean(false);
            final ClusterInfoSession session = factory.createSession();
            
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            String clusterPath = createClusterLevel(cid,session);
            assertNotNull(pass,clusterPath);
            
            ClusterInfoWatcher watcher = new ClusterInfoWatcher()
            {
               @Override
               public void process()
               {
                  processCalled.set(true);
               }
            };
            
            assertTrue(session.exists(cid.asPath(), watcher));

            String data = "HelloThere";
            session.setData(clusterPath,data);
            
            assertTrue(poll(5000, null, new Condition<Object>()
            {
               @Override
               public boolean conditionMet(Object o) { return processCalled.get(); }
            }));
            
            processCalled.set(false);
            
            String cdata = (String)session.getData(clusterPath,watcher);
            assertEquals(pass,data,cdata);
            
            // add the null watcher ...
            cdata = (String)session.getData(clusterPath,null);
            assertEquals(pass,data,cdata);
            
            // but makes sure that it doesn't affect the callback
            Thread.sleep(500); //just in case.
            assertFalse(processCalled.get());

            data += "2";
            session.setData(clusterPath,data);
            
            assertTrue(poll(5000, null, new Condition<Object>()
            {
               @Override
               public boolean conditionMet(Object o) { return processCalled.get(); }
            }));
            
            cdata = (String)session.getData(clusterPath,null);
            assertEquals(pass,data,cdata);
            session.stop();
         }
         
      });
   }
   

}
