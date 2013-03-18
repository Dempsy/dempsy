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

package com.nokia.dempsy.cluster.zookeeper;

import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.zookeeper.FullApplication.MyAdaptor;
import com.nokia.dempsy.cluster.zookeeper.FullApplication.MyMp;
import com.nokia.dempsy.cluster.zookeeper.FullApplication.MyRankMp;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.coda.MetricGetters;
import com.nokia.dempsy.router.CurrentClusterCheck;

public class TestFullApp
{
   private static final String dempsyConfig = "fullApp/Dempsy.xml";
   private static final String clusterManager = "testDempsy/ClusterInfo-ZookeeperActx.xml";
   private static final String transport = "testDempsy/Transport-TcpActx.xml";
   private static final long baseTimeoutMillis = 10000;
   
   private static String[] ctx = new String[4];
   static {
      ctx[0] = dempsyConfig;
      ctx[1] = clusterManager;
      ctx[2] = transport;
      ctx[3] = "fullApp/DempsyApplicationContext-FullApp.xml";
   }

   private static Logger logger = LoggerFactory.getLogger(TestFullApp.class);
   
   private static InitZookeeperServerBean zkServer = null;

   @Before
   public void setupZookeeperSystemVars() throws IOException
   {
      System.setProperty("application", "test-app");
      System.setProperty("cluster", "test-cluster2");
      zkServer = new InitZookeeperServerBean();
      FullApplication.mpOnly = false;
      System.setProperty("testFullApp.totalShards", "20");
   }
   
   @After
   public void shutdownZookeeper()
   {
      zkServer.stop();
   }
   
   @Test
   public void testStartStop() throws Throwable
   {
      ClassPathXmlApplicationContext actx = null;
      Dempsy dempsy = null;

      try
      {   
         logger.debug("Starting up the appliction context ...");
         actx = new ClassPathXmlApplicationContext(ctx);
         actx.registerShutdownHook();

         dempsy = (Dempsy)actx.getBean("dempsy");
         dempsy.start();

         final FullApplication app = (FullApplication)actx.getBean("app");

         // this checks that the throughput works.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return app.finalMessageCount.get() > 100;
            }
         }));
      }
      finally
      {
         if (dempsy != null)
            dempsy.stop();

         if (actx != null)
            actx.close();

         if (dempsy != null)
            assertTrue(dempsy.waitToBeStopped(baseTimeoutMillis));
      }
   }

   private ZookeeperSession zookeeperCluster = null;
   
   @Test
   public void testStartForceMpDisconnectStop() throws Throwable
   {
      ClassPathXmlApplicationContext actx = null;
      Dempsy dempsy = null;

      try
      {   
         logger.debug("Starting up the appliction context ...");
         actx = new ClassPathXmlApplicationContext(ctx);
         actx.registerShutdownHook();

         final FullApplication app = (FullApplication)actx.getBean("app");

         dempsy = (Dempsy)actx.getBean("dempsy");

         // Override the cluster session factory to keep track of the sessions asked for.
         // This is so that I can grab the ZookeeperSession that's being instantiated by
         // the MyMp cluster.
         zookeeperCluster = null;
         dempsy.setClusterSessionFactory(
               new ZookeeperSessionFactory(System.getProperty("zk_connect"), 5000)
               {
                  int sessionCount = 0;

                  @Override
                  public synchronized ClusterInfoSession createSession() throws ClusterInfoException
                  {
                     sessionCount++;
                     ClusterInfoSession ret = super.createSession();

                     if (sessionCount == 2)
                        zookeeperCluster = (ZookeeperSession)ret;
                     return ret;
                  }
               });

         dempsy.start();

         Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyAdaptor.class.getSimpleName()));
         Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0);
         final StatsCollector collector = node.getStatsCollector();

         // this checks that the throughput works.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return app.finalMessageCount.get() > 10;
            }
         }));

         assertNotNull(zookeeperCluster);

         assertEquals(0,((MetricGetters)collector).getDiscardedMessageCount());
         assertEquals(0,((MetricGetters)collector).getMessageFailedCount());

         // ok ... so now we have stuff going all the way through. let's kick
         // the middle Mp's zookeeper cluster and see what happens.
         ZooKeeper origZk = zookeeperCluster.zkref.get();
         long sessionid = origZk.getSessionId();
         ZooKeeper killer = new ZooKeeper(System.getProperty("zk_connect"),5000, new Watcher() { @Override public void process(WatchedEvent arg0) { } }, sessionid, null);
         killer.close(); // tricks the server into expiring the other session

//         // we should be getting failures now ... 
//         // but it's possible that it can reconnect prior to actually seeing an error so if this 
//         //   fails frequently we need to remove this test.
//         assertTrue(poll(baseTimeoutMillis, app, new Condition()
//         {
//            @Override
//            public boolean conditionMet(Object o)
//            {
//               return collector.getMessageFailedCount() > 1;
//            }
//         }));

         //... and then recover.

         // get the MyMp prototype
         cluster = dempsy.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyMp.class.getSimpleName()));
         node = cluster.getNodes().get(0);
         final MyMp prototype = (MyMp)node.getMpContainer().getPrototype();

         // so let's see where we are
         final long interimMessageCount = prototype.myMpReceived.get();

         // and now we should eventually get more as the session recovers.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return prototype.myMpReceived.get() > interimMessageCount + 100;
            }
         }));
      }
      finally
      {
         if (dempsy != null)
            dempsy.stop();
         
         if (actx != null)
            actx.close();

         if (dempsy != null)
            assertTrue(dempsy.waitToBeStopped(baseTimeoutMillis));
      }
   }

   @Test
   public void testStartForceMpDisconnectWithStandby() throws Throwable
   {
      // by default the testFullApp.totalShards is set to 20. To NOT do elasticity we need to
      // set it to 1 - the same as the minNode count.
      System.setProperty("testFullApp.totalShards", "1");
      runTestStartForceMpDisconnectWithRedundancy();
   }
   
   @Test
   public void testStartForceMpDisconnectWithElasticity() throws Throwable
   {
      runTestStartForceMpDisconnectWithRedundancy();
   }
   
   private void runTestStartForceMpDisconnectWithRedundancy() throws Throwable
   {
      ClassPathXmlApplicationContext actx = null;
      ClassPathXmlApplicationContext actx2 = null;
      Dempsy dempsy = null;
      Dempsy dempsy2 = null;
      
      try
      {
         logger.debug("Starting up the appliction context ...");
         actx = new ClassPathXmlApplicationContext(ctx);
         actx.registerShutdownHook();

         final FullApplication app = (FullApplication)actx.getBean("app");

         dempsy = (Dempsy)actx.getBean("dempsy");
         
         ZookeeperSessionFactory zsf = new ZookeeperSessionFactory(System.getProperty("zk_connect"), 5000)
         {
            int sessionCount = 0;

            @Override
            public synchronized ClusterInfoSession createSession() throws ClusterInfoException
            {
               sessionCount++;
               ClusterInfoSession ret = super.createSession();

               if (sessionCount == 2)
                  zookeeperCluster = (ZookeeperSession)ret;
               return ret;
            }
         }; 

         // Override the cluster session factory to keep track of the sessions asked for.
         // This is so that I can grab the ZookeeperSession that's being instantiated by
         // the MyMp cluster.
         zookeeperCluster = null;
         dempsy.setClusterSessionFactory(zsf);

         dempsy.start();

         Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyAdaptor.class.getSimpleName()));
         Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0);
         final StatsCollector collector = node.getStatsCollector();
         
         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy));
         
         // we are going to create another node of the MyMp via a test hack
         FullApplication.mpOnly = true;
         actx2 = new ClassPathXmlApplicationContext(ctx);
         actx2.registerShutdownHook();
         dempsy2 = (Dempsy)actx2.getBean("dempsy");
         dempsy2.setClusterSessionFactory(zsf);
         dempsy2.start();
         
         cluster = dempsy.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyMp.class.getSimpleName()));
         Dempsy.Application.Cluster.Node mpnode = cluster.getNodes().get(0);

         // this checks that the throughput works.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return app.finalMessageCount.get() > 10;
            }
         }));

         assertNotNull(zookeeperCluster);

         assertEquals(0,((MetricGetters)collector).getDiscardedMessageCount());
         assertEquals(0,((MetricGetters)collector).getMessageFailedCount());

         // ok ... so now we have stuff going all the way through. let's kick
         // the middle Mp's zookeeper cluster and see what happens.
         ZooKeeper origZk = zookeeperCluster.zkref.get();
         origZk.close(); // this should kill it.
         
         // but just to be sure actually stop the node.
         mpnode.stop();

//         // we should be getting failures now ... 
//         // but it's possible that it can reconnect prior to actually seeing an error so if this 
//         //   fails frequently we need to remove this test.
//         assertTrue(poll(baseTimeoutMillis, app, new Condition()
//         {
//            @Override
//            public boolean conditionMet(Object o)
//            {
//               return collector.getMessageFailedCount() > 1;
//            }
//         }));

         //... and then recover.

         // get the MyMp prototype
         cluster = dempsy2.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyMp.class.getSimpleName()));
         node = cluster.getNodes().get(0);
         final MyMp prototype = (MyMp)node.getMpContainer().getPrototype();

         // so let's see where we are
         final long interimMessageCount = prototype.myMpReceived.get();

         // and now we should eventually get more as the session recovers.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return prototype.myMpReceived.get() > interimMessageCount + 100;
            }
         }));

      }
      finally
      {
         if (dempsy != null)
            dempsy.stop();
         
         if (dempsy2 != null)
            dempsy2.stop();
         
         if (actx != null)
            actx.close();

         if (actx2 != null)
            actx2.close();

         if (dempsy != null)
            assertTrue(dempsy.waitToBeStopped(baseTimeoutMillis));
         
         if (dempsy2 != null)
            assertTrue(dempsy2.waitToBeStopped(baseTimeoutMillis));

      }

   }
   
   public static class CheckCluster implements CurrentClusterCheck
   {
      public static ClusterId toCheckAgainst = null;
      
      @Override
      public boolean isThisNodePartOfCluster(ClusterId clusterToCheck)
      {
         return toCheckAgainst.equals(clusterToCheck);
      }

      @Override
      public boolean isThisNodePartOfApplication(String applicationName)
      {
         return toCheckAgainst.getApplicationName().equals(applicationName);
      }
      
   }
   
   public static class DempsyHolder
   {
      public ClassPathXmlApplicationContext actx = null;
      public ClusterId clusterid = null;
      public Dempsy dempsy = null;
   }

   @Test
   public void testSeparateClustersInOneVm() throws Throwable
   {
      // now start each cluster
      ctx[0] = "fullApp/Dempsy-FullUp.xml";
      Map<ClusterId,DempsyHolder> dempsys = new HashMap<ClusterId,DempsyHolder>();

      try
      {
         ApplicationDefinition ad = new FullApplication().getTopology();
         ad.initialize();

         List<ClusterDefinition> clusters = ad.getClusterDefinitions();
         for (int i = clusters.size() - 1; i >= 0; i--)
         {
            ClusterDefinition cluster = clusters.get(i);
            CheckCluster.toCheckAgainst = cluster.getClusterId();

            DempsyHolder cur = new DempsyHolder();
            cur.clusterid = cluster.getClusterId();
            cur.actx = new ClassPathXmlApplicationContext(ctx);
            cur.actx.registerShutdownHook();
            cur.dempsy =  (Dempsy)cur.actx.getBean("dempsy");
            cur.dempsy.start();
            dempsys.put(cluster.getClusterId(), cur);
         }

         // get the last FullApplication in the processing chain.
         ClassPathXmlApplicationContext actx = dempsys.get(new ClusterId(FullApplication.class.getSimpleName(),MyRankMp.class.getSimpleName())).actx;
         final FullApplication app = (FullApplication)actx.getBean("app");

         // this checks that the throughput works.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return app.finalMessageCount.get() > 100;
            }
         }));
      }
      finally
      {
         ctx[0] = dempsyConfig;
         
         for (DempsyHolder cur : dempsys.values())
         {
            cur.dempsy.stop();
            cur.actx.close();
         }
      }
   }

   @Test
   public void testFailoverWithStandby() throws Throwable
   {
      // by default the testFullApp.totalShards is set to 20. To NOT do elasticity we need to
      // set it to 1 - the same as the minNode count.
      runTestFailover(false);
   }
   
   @Test
   public void testFailoverWithElasticity() throws Throwable
   {
      runTestFailover(true);
   }
   
   public void runTestFailover(boolean elastic) throws Throwable
   {
      // we need to set the totalShards = the minNodeCount to disable elasiticity.
      // The default is totalShards = 20 from the @Before method
      if (!elastic)
         System.setProperty("testFullApp.totalShards", "1");

      // now start each cluster
      ctx[0] = "fullApp/Dempsy-FullUp.xml";
      Map<ClusterId,DempsyHolder> dempsys = new HashMap<ClusterId,DempsyHolder>();
      DempsyHolder spare = new DempsyHolder();

      try
      {
         ApplicationDefinition ad = new FullApplication().getTopology();
         ad.initialize();

         List<ClusterDefinition> clusters = ad.getClusterDefinitions();
         for (int i = clusters.size() - 1; i >= 0; i--)
         {
            ClusterDefinition cluster = clusters.get(i);
            CheckCluster.toCheckAgainst = cluster.getClusterId();

            DempsyHolder cur = new DempsyHolder();
            cur.clusterid = cluster.getClusterId();
            cur.actx = new ClassPathXmlApplicationContext(ctx);
            cur.actx.registerShutdownHook();
            cur.dempsy =  (Dempsy)cur.actx.getBean("dempsy");
            cur.dempsy.start();
            dempsys.put(cluster.getClusterId(), cur);
         }
         
         // get the last FullApplication in the processing chain.
         ClassPathXmlApplicationContext actx = dempsys.get(new ClusterId(FullApplication.class.getSimpleName(),MyRankMp.class.getSimpleName())).actx;
         final FullApplication app = (FullApplication)actx.getBean("app");

         // this checks that the throughput works.
         assertTrue(poll(baseTimeoutMillis * 5, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return app.finalMessageCount.get() > 100;
            }
         }));
         
         // now start another MyMp cluster.
         spare = new DempsyHolder();
         spare.clusterid = new ClusterId(FullApplication.class.getSimpleName(),MyMp.class.getSimpleName());
         CheckCluster.toCheckAgainst = spare.clusterid;
         spare.actx = new ClassPathXmlApplicationContext(ctx);
         spare.dempsy = (Dempsy)spare.actx.getBean("dempsy");
         spare.dempsy.start();

         Dempsy.Application.Cluster cluster = spare.dempsy.getCluster(spare.clusterid);
         Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0);
         final StatsCollector spareNodeStatsCollector = node.getStatsCollector();
         
         // we are going to create another node of the MyMp via a test hack
         cluster = spare.dempsy.getCluster(new ClusterId(FullApplication.class.getSimpleName(),MyMp.class.getSimpleName()));
         node = cluster.getNodes().get(0);
         final MyMp spareprototype = (MyMp)node.getMpContainer().getPrototype();

         // If we're elastic then we will get data to the spare. So we need to test for that.
         if (elastic)
         {
            assertTrue(TestUtils.poll(baseTimeoutMillis, (MetricGetters)spareNodeStatsCollector, 
                  new Condition<MetricGetters>()
                  {
                     @Override
                     public boolean conditionMet(MetricGetters o) throws Throwable {
                        return o.getDispatchedMessageCount() > 10;// we should eventually see messages here.
                     }
                  }));
            assertTrue(TestUtils.poll(baseTimeoutMillis, spareprototype, 
                  new Condition<MyMp>()
                  {
                     @Override
                     public boolean conditionMet(MyMp o) throws Throwable {
                        return o.myMpReceived.get() > 10;// we should eventually see messages here.
                     }
                  }));
         }
         // If we're not elastic then the spare should be in backup mode, and we should check for that.
         else
         {
            Thread.sleep(100); // give some time for messages to wrongly be propagated
            assertEquals(0,((MetricGetters)spareNodeStatsCollector).getDispatchedMessageCount());
            assertEquals(0,spareprototype.myMpReceived.get());
         }
         
         // now bring down the original
         DempsyHolder original = dempsys.get(spare.clusterid);
         final MyMp originalprototype = (MyMp)original.dempsy.getCluster(spare.clusterid).getNodes().get(0).getMpContainer().getPrototype();
         final long originalNumMessages = originalprototype.myMpReceived.get();
         
         // makes sure the message count is still advancing
         assertTrue(poll(baseTimeoutMillis, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return originalprototype.myMpReceived.get() > originalNumMessages;
            }
         }));
         
         // check one more time ... if not elastic
         if (!elastic)
            assertEquals(0,spareprototype.myMpReceived.get());

         // now stop the original ... the spare should pick up.
         original.dempsy.stop();

         // there's a race condition between the stop returning and the last message
         // being processed.
         
         // we need to check that a certain amount of time passes during which no more messages have been received.
         final long numMillisecondsWithoutAMessage = 500; // if we haven't seen a message in 1/2 second then we
                                                          //  will assume that the messages have stopped.
         
         // now we wait until at least numMillisecondsWithoutAMessage goes by without the myMpReceived
         //  being incremented. This must happen within the baseTimeoutMillis or this check is
         //   considered failed.
         poll(baseTimeoutMillis + numMillisecondsWithoutAMessage, originalprototype, new Condition<Object>()
         {
            long startCheckingTime = System.currentTimeMillis();
            long lastMessage = originalprototype.myMpReceived.get();
            
            @Override
            public boolean conditionMet(Object o)
            {
               if (originalprototype.myMpReceived.get() != lastMessage)
               {
                  startCheckingTime = System.currentTimeMillis();
                  lastMessage = originalprototype.myMpReceived.get();
                  return false;
               }
               else
                  return (System.currentTimeMillis() - startCheckingTime) > numMillisecondsWithoutAMessage;
            }
         });
         
         // now check to see if the new one picked up.
         assertTrue(poll(baseTimeoutMillis, app, new Condition<Object>()
         {
            @Override
            public boolean conditionMet(Object o)
            {
               return spareprototype.myMpReceived.get() > 10;
            }
         }));

      }
      finally
      {
         ctx[0] = dempsyConfig;
         
         for (DempsyHolder cur : dempsys.values())
         {
            cur.dempsy.stop();
            cur.actx.close();
         }
         
         if (spare.dempsy != null)
            spare.dempsy.stop();
         
         if (spare.actx != null)
            spare.actx.close();
      }
   }
}
