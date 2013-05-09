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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.tcp.TcpTransport;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollector;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollectorFactory;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;
import com.nokia.dempsy.router.RegExClusterCheck;
import com.nokia.dempsy.router.Router;
import com.nokia.dempsy.router.TestUtilRouter;
import com.nokia.dempsy.router.microshard.MicroShardUtils;
import com.nokia.dempsy.serialization.java.JavaSerializer;

/**
 * The goal here is to make sure the cluster is always consistent even if it looses 
 * the zookeeper session connection or doesn't have it to begin with.
 */
public class TestZookeeperClusterResilience
{
   public static final String appname = TestZookeeperClusterResilience.class.getSimpleName();
   private static Logger logger = LoggerFactory.getLogger(TestZookeeperClusterResilience.class);
   static private final long baseTimeoutMillis = 20000;
   
   private int port;
   
   @Before
   public void setup() throws IOException
   {
      port = ZookeeperTestServer.findNextPort();
      logger.debug("Running zookeeper test server on port " + port);
   }
   
   public static abstract class TestWatcher implements ClusterInfoWatcher
   {
      AtomicBoolean called = new AtomicBoolean(false);
      ZookeeperSession session;
      
      public TestWatcher(ZookeeperSession session) { this.session = session; } 
      
   }
   
   volatile boolean connected = false;
   
   @Test
   public void testBouncingServer() throws Throwable
   {
      runBouncingServerTest("testBouncingServer",false);
   }
   
   @Test
   public void testBouncingServerWithCleanDataDir() throws Throwable
   {
      runBouncingServerTest("testBouncingServerWithCleanDataDir",true);
   }

   private void runBouncingServerTest(String testName, boolean deleteDataDir) throws Throwable
   {
      ZookeeperTestServer server = new ZookeeperTestServer();
      ZookeeperSession session = null;
      final ClusterId clusterId = new ClusterId(appname,testName);
      
      try
      {
         server.start();

         ZookeeperSessionFactory factory = new ZookeeperSessionFactory("127.0.0.1:" + port,5000);
         session = (ZookeeperSession)factory.createSession();
         final ZookeeperSession cluster = session;
         final MicroShardUtils u = new MicroShardUtils(clusterId);
         u.mkAllPersistentAppDirs(session, null);
         TestWatcher callback = new TestWatcher(session)
         {
            
            @Override
            public void process()
            {
               boolean done = false;
               while (!done)
               {
                  done = true;
                  
                  try
                  {
                     if (session.getSubdirs(u.getShardsDir(), this).size() == 0)
                        session.mkdir(u.getShardsDir() + "/slot1",DirMode.EPHEMERAL);
                     called.set(true);
                  }
                  catch(ClusterInfoException.NoNodeException e)
                  {
                     try
                     {
                        new MicroShardUtils(clusterId).mkAllPersistentAppDirs(session,null);
                        done = false;
                     }
                     catch (ClusterInfoException e1)
                     {
                        throw new RuntimeException(e1);
                     }
                  }
                  catch(ClusterInfoException e)
                  {
                     // this will fail when the connection is severed... that's ok.
                  }
               }
            }

         };

         cluster.exists(u.getShardsDir(), callback);
         callback.process();
         
         // create another session and look
         ZookeeperSession session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new MicroShardUtils(new ClusterId(appname,testName)).getShardsDir(), null).size());
         session2.stop();

         // kill the server.
         server.shutdown(deleteDataDir);

         // reset the flags
         callback.called.set(false);

         // restart the server
         server.start(deleteDataDir);
         
         // wait for the call
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new TestUtils.Condition<TestWatcher>()
         {
            @Override
            public boolean conditionMet(TestWatcher o) { return o.called.get(); }
         }));

         // get the view from a new session.
         session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new MicroShardUtils(new ClusterId(appname,testName)).getShardsDir(), null).size());
         session2.stop();
      }
      finally
      {
         if (server != null)
            server.shutdown();
         
         if (session != null)
            session.stop();
      }
   }

   @Test
   public void testNoServerOnStartup() throws Throwable
   {
      // create a session factory
      ZookeeperSessionFactory factory = new ZookeeperSessionFactory("127.0.0.1:" + port,5000);
      
      // create a session from the session factory
      final ZookeeperSession session = (ZookeeperSession)factory.createSession();
      
      ClusterId clusterId = new ClusterId(appname,"testNoServerOnStartup");
      
      // hook a test watch to make sure that callbacks work correctly
      final TestWatcher callback = new TestWatcher(session)
      {
         @Override public void process() { called.set(true); }
      };
      
      // now accessing the cluster should get us an error.
      MicroShardUtils u = new MicroShardUtils(clusterId);
      boolean gotCorrectError = false;
      try { session.getSubdirs(u.getShardsDir(), callback); } catch (ClusterInfoException e) { gotCorrectError = true; }
      assertTrue(gotCorrectError);
      
      // now lets startup the server.
      ZookeeperTestServer server = null;
      try
      {
         server = new ZookeeperTestServer();
         server.start();
         
         // create a cluster from the session
         new MicroShardUtils(clusterId).mkAllPersistentAppDirs(session,null);
         
         // wait until this works.
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new Condition<TestWatcher>() {
            @Override public boolean conditionMet(TestWatcher o){  return o.called.get(); }
         }));
         
         callback.called.set(false); // reset the callbacker ...
         
         // now see if the cluster works.
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new Condition<TestWatcher>() {
            @Override public boolean conditionMet(TestWatcher o){  return !o.called.get(); }
         }));

         session.getSubdirs(new MicroShardUtils(clusterId).getShardsDir(), callback);
         
         ZooKeeper origZk = session.zkref.get();
         ZookeeperTestServer.forceSessionExpiration(origZk);
         
         // wait for the callback
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new Condition<TestWatcher>() {
            @Override public boolean conditionMet(TestWatcher o){  return o.called.get(); }
         }));
         
         // unfortunately I cannot check the getActiveSlots for failure because there's a race condition I can't fix.
         //  No matter how fast I check it's possible that it's okay again OR that allSlots hasn't been cleared.
         // 
         // however, they should eventually recover.
         assertTrue(TestUtils.poll(baseTimeoutMillis, new MicroShardUtils(clusterId), new Condition<MicroShardUtils>()
         {
            @Override
            public boolean conditionMet(MicroShardUtils o) throws Throwable {  
               try { session.getSubdirs(o.getShardsDir(), callback); return true; } catch (ClusterInfoException e) {  }
               return false;
            }
         }));
         
         // And join should work
         assertTrue(TestUtils.poll(baseTimeoutMillis, new MicroShardUtils(clusterId), new Condition<MicroShardUtils>()
         {
            @Override
            public boolean conditionMet(MicroShardUtils o) throws Throwable {  
               try { session.mkdir(o.getShardsDir() + "/join-1", DirMode.EPHEMERAL); return true; } catch (ClusterInfoException e) {  }
               return false;
            }
         }));

      }
      finally
      {
         if (server != null)
            server.shutdown();
         
         if (session != null)
            session.stop();
      }
   }
   
   @Test
   public void testSessionExpired() throws Throwable
   {
      // now lets startup the server.
      ZookeeperTestServer server = null;
      ZookeeperSession session = null;
      
      try
      {
         server = new ZookeeperTestServer();
         server.start();

         session =  new ZookeeperSession("127.0.0.1:" + port,5000);

         final ClusterId clusterId = new ClusterId(appname,"testSessionExpired");
         final MicroShardUtils u = new MicroShardUtils(clusterId);
         u.mkAllPersistentAppDirs(session,null);
         TestWatcher callback = new TestWatcher(session)
         {
            @Override
            public void process()
            {
               try
               {
                  called.set(true);
                  logger.trace("process called on TestWatcher.");
                  session.exists(u.getClusterDir(), this);
                  session.getSubdirs(u.getClusterDir(), this);
               }
               catch (ClusterInfoException cie)
               {
                  throw new RuntimeException(cie);
               }
            }
 
         };
         
         // now see if the cluster works.
         callback.process(); // this registers the session with the callback as the Watcher
         
         // now reset the condition
         callback.called.set(false);
         
         // and eventually a callback
         ZookeeperTestServer.forceSessionExpiration(session.zkref.get());

         // we should see the session expiration in a callback
         assertTrue(poll(5000,callback,new Condition<TestWatcher>() {  @Override public boolean conditionMet(TestWatcher o) {  return o.called.get(); } }));

         // and eventually a reconnect
         assertTrue(poll(5000,callback,new Condition<TestWatcher>() 
         {  
            @Override public boolean conditionMet(TestWatcher o) 
            {
               try
               {
                  o.process();
                  return true;
               }
               catch (Throwable th)
               {
                  return false;
               }
            } 
         }));
         
         // now we should be able to recheck
         u.mkAllPersistentAppDirs(session,null);
         assertTrue(session.exists(u.getShardsDir(), callback));
      }
      finally
      {
         if (server != null)
            server.shutdown();
         
         if (session != null)
            session.stop();
      }
   }
   
   //------------------------------------------------------------------------------
   // here is a complete non-spring, non-DI Dempsy instantiation
   //------------------------------------------------------------------------------
   private static Dempsy getDempsyFor(ClusterId clusterId, ClusterDefinition cd)
   {
      Dempsy dempsy = new Dempsy();
      List<ClusterDefinition> cds = new ArrayList<ClusterDefinition>(1);
      cds.add(cd);
      dempsy.setClusterDefinitions(cds);
      initDempsy(dempsy,clusterId);
      return dempsy;
   }
   
   private static Dempsy getDempsyFor(ClusterId clusterId, ApplicationDefinition ad) throws Throwable
   {
      List<ApplicationDefinition> ads = new ArrayList<ApplicationDefinition>();
      ads.add(ad);
      
      Dempsy dempsy = new Dempsy();
      dempsy.setApplicationDefinitions(ads);
      initDempsy(dempsy,clusterId);
      return dempsy;
   }
   
   private static BasicStatsCollector curCollector = null;
   
   private static void initDempsy(Dempsy dempsy, ClusterId clusterId)
   {
      dempsy.setClusterCheck(new RegExClusterCheck(clusterId));
      dempsy.setDefaultRoutingStrategy(new DecentralizedRoutingStrategy(20, 1));
      dempsy.setDefaultSerializer(new JavaSerializer<Object>());
      dempsy.setDefaultStatsCollectorFactory(new BasicStatsCollectorFactory()
      {
         @Override
         public StatsCollector createStatsCollector(ClusterId clusterId, Destination listenerDestination)
         {
            if (curCollector == null)
               curCollector = new BasicStatsCollector();
            return curCollector;
         }

      });
      dempsy.setDefaultTransport(new TcpTransport());
   }
   //------------------------------------------------------------------------------
   
   public static class AnotherAdaptor implements Adaptor
   {
      public Dispatcher dispatcher;
      
      @Override
      public void setDispatcher(Dispatcher dispatcher){ this.dispatcher = dispatcher; }

      @Override
      public void start() { }

      @Override
      public void stop() { }
   }
   
   // inherited message
   public static class MyMessageCountChild extends FullApplication.MyMessageCount
   {
      private static final long serialVersionUID = 1L;
      public static volatile AtomicLong hitCount = new AtomicLong(0);
      
      public MyMessageCountChild(long v) { super(v); }
      
      @Override
      public void hitMe()
      {
         hitCount.incrementAndGet();
      }
   }
   
   @MessageProcessor
   public static class MyRankAgain implements Cloneable
   {
      public static AtomicLong called = new AtomicLong(0);
      
      @MessageHandler
      public void handleMessage(MyMessageCountChild m) 
      {
//         System.out.println("Got One In Child!");
         m.hitMe(); 
         called.incrementAndGet();
      }
      
      @Override
      public MyRankAgain clone() throws CloneNotSupportedException
      {
         return (MyRankAgain)super.clone();
      }
   }
   
   @Test
   public void testSessionExpiredWithFullAppPlusDynamicAdditions() throws Throwable
   {
      logger.trace("Running test testSessionExpiredWithFullAppPlusDynamicAdditions");
      
      // now lets startup the server.
      ZookeeperTestServer server = null;
      final AtomicReference<ZookeeperSession> sessionRef = new AtomicReference<ZookeeperSession>();
      ZookeeperSession session = null;
      final AtomicLong processCount = new AtomicLong(0);
      
      Dempsy[] dempsy = new Dempsy[5];
      try
      {
         server = new ZookeeperTestServer();
         server.start();

         session = new ZookeeperSession("127.0.0.1:" + port,5000) {
            @Override
            public ZookeeperSession.WatcherProxy makeWatcherProxy(ClusterInfoWatcher watcher)
            {
               processCount.incrementAndGet();
               return super.makeWatcherProxy(watcher);
            }
         };
         sessionRef.set(session);

         final FullApplication app = new FullApplication();
         ApplicationDefinition ad = app.getTopology();

         dempsy[0] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyAdaptor.class.getSimpleName()),ad);
         dempsy[0].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));

         dempsy[1] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyMp.class.getSimpleName()),ad);
         dempsy[1].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));

         dempsy[2] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyRankMp.class.getSimpleName()),ad);
         
         dempsy[2].setClusterSessionFactory(new ClusterInfoSessionFactory()
         {
            @Override
            public ClusterInfoSession createSession() throws ClusterInfoException
            {
               return sessionRef.get();
            }
         });
         
         // start everything in reverse order
         dempsy[2].start();
         dempsy[1].start();
         dempsy[0].start();

         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy[0]));
         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy[1]));
         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy[2]));

         // make sure the final count is incrementing
         long curCount = app.finalMessageCount.get();
         assertTrue(poll(baseTimeoutMillis,curCount,new Condition<Long>(){
            @Override
            public boolean conditionMet(Long o)
            {
               return app.finalMessageCount.get() > (o + 100L);
            }
            
         }));

         logger.trace("Killing zookeeper");
         ZooKeeper origZk = session.zkref.get();
         ZookeeperTestServer.forceSessionExpiration(origZk);
         logger.trace("Killed zookeeper");
         
         // wait for the current session to go invalid
         assertTrue(poll(baseTimeoutMillis, origZk, new Condition<ZooKeeper>()
         {
            @Override
            public boolean conditionMet(ZooKeeper o) { return !o.getState().isAlive(); }
         }));
         
         // make sure the final count is STILL incrementing
         curCount = app.finalMessageCount.get();
         assertTrue(poll(baseTimeoutMillis,curCount,new Condition<Long>(){
            @Override
            public boolean conditionMet(Long o)
            {
               return app.finalMessageCount.get() > (o + 100L);
            }
         }));
         
         //====================================================================
         // This test is now extended to use the dynamic topology functionality
         //====================================================================
         
         // now, see if I can add a dynamic cluster
         AnotherAdaptor anotherAdaptor = new AnotherAdaptor();
         ClusterId cid2 = new ClusterId(FullApplication.class.getSimpleName(),AnotherAdaptor.class.getSimpleName());
         ClusterDefinition cd2 = new ClusterDefinition(cid2.getMpClusterName());
         cd2.setApplicationDefinition(app.createBaseApplicationDefinition());
         cd2.setAdaptor(anotherAdaptor);
         
         // create a start a new dempsy instance
         dempsy[3] = getDempsyFor(cid2,cd2);
         dempsy[3].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));
         dempsy[3].start();
         
         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy[3]));
         
         // wait for the dispatcher to be set
         assertTrue(poll(baseTimeoutMillis,anotherAdaptor,new Condition<AnotherAdaptor>() {
            @Override
            public boolean conditionMet(AnotherAdaptor o) { return o.dispatcher != null; }
         }));
         
         // Now send a message.
         MyMessageCountChild.hitCount.set(0);
         MyMessageCountChild msg1 = new MyMessageCountChild(-1);
         anotherAdaptor.dispatcher.dispatch(msg1);
         
         // even though this message inherits from the base MyMessageCount class
         //  it should make it through to the MyRankMp which will call hitMe
         //  and it will cause the static hitCount to increment.
         assertTrue(poll(baseTimeoutMillis,null,new Condition<Object>() {
            @Override
            public boolean conditionMet(Object o) { return MyMessageCountChild.hitCount.get() > 0; }
         }));
         
         // Make sure we've seen only one.
         Thread.sleep(10);
         assertEquals(1,MyMessageCountChild.hitCount.get());
         
         // Now add a new Mp that will also receive Rank
         MyMessageCountChild.hitCount.set(0);
         
         // ... but stop the zk server
         server.shutdown();
         
         MyRankAgain mp3 = new MyRankAgain();
         ClusterId cid3 = new ClusterId(FullApplication.class.getSimpleName(),MyRankAgain.class.getSimpleName());
         ClusterDefinition cd3 = new ClusterDefinition(cid3.getMpClusterName());
         cd3.setApplicationDefinition(app.createBaseApplicationDefinition());
         cd3.setMessageProcessorPrototype(mp3);

         // create a start a new dempsy instance
         dempsy[4] = getDempsyFor(cid3,cd3);
         dempsy[4].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));
         dempsy[4].start();
         
         Thread.sleep(10);
         server = new ZookeeperTestServer();
         server.start();
         
         // This wont initialize until the server is available.
         assertTrue(TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy[4]));

         app.finalMessageCount.set(0);
         MyRankAgain.called.set(0);
         
         // poll until messages get through
         assertTrue(poll(2*baseTimeoutMillis,anotherAdaptor,new Condition<AnotherAdaptor>() {
            @Override
            public boolean conditionMet(AnotherAdaptor o) { o.dispatcher.dispatch(new MyMessageCountChild(-1)); return (MyRankAgain.called.get() > 10) && app.finalMessageCount.get() > 10; }
         }));
         
         // now sever the original adaptor from the processing chain 
         //  by shutting down the only cluster that it was sending data
         //  to.
         dempsy[1].stop();

         // there's a race condition here ... some messages may still
         // trickle through from the above sending. This should avoid that
         // MOST of the time.
         assertTrue(dempsy[1].waitToBeStopped(baseTimeoutMillis));
         
         // now reset the counter
         MyMessageCountChild.hitCount.set(0);
         Thread.sleep(100);
         
         // This should remain at 0.
         assertEquals(0,MyMessageCountChild.hitCount.get());
         
         // This test periodically fails because there is a race condition where dempsy[4] isn't 
         // yet seen by the OutboundManager for the anotherAdaptor (dempsy[3]). So let's wait for the Outbound to
         // be set up before we try this.
         Router router3 = TestUtils.getRouter(dempsy[3],FullApplication.class.getSimpleName(),AnotherAdaptor.class.getSimpleName());
         assertTrue(TestUtils.poll(baseTimeoutMillis,router3,new Condition<Router>()
         {
            @Override
            public boolean conditionMet(Router o) throws Throwable { return 2 == TestUtilRouter.numberOfKnownDestinationsForMessage(o, MyMessageCountChild.class); }
         }));
         
         // now that the destinations are known we need to wait until the outbounds are initialized.
         assertTrue(TestUtils.poll(baseTimeoutMillis,router3,new Condition<Router>()
         {
            @Override
            public boolean conditionMet(Router o) throws Throwable { return TestUtilRouter.allOutboundsInitialized(o, MyMessageCountChild.class); }
         }));
         
         
         logger.trace("********* Sending one message. Should result in two hits. ****************");

         // send a messages. This should result in 2 Mps getting the message.
         // one from dempsy[2] and dempsy[4]
         anotherAdaptor.dispatcher.dispatch(new MyMessageCountChild(-1));
         
         logger.trace("********* Sent one message. Should result in two hits. ****************");

         // now, because there are 2 ultimate destinations for this message, the hitCount should got to 2
         assertTrue(poll(baseTimeoutMillis,null,new Condition<Object>() {
            @Override
            public boolean conditionMet(Object o) { return MyMessageCountChild.hitCount.get() == 2; }
         }));
         Thread.sleep(100);
         assertEquals(2,MyMessageCountChild.hitCount.get());
      }
      finally
      {
         logger.trace("Finishing test testSessionExpiredWithFullAppPlusDynamicAdditions");

         if (server != null)
            server.shutdown();
         
         if (session != null)
            session.stop();
         
         for (int i = 0; i < dempsy.length; i++)
            if (dempsy[i] != null)
               dempsy[i].stop();

         for (int i = 0; i < dempsy.length; i++)
            if (dempsy[i] != null)
               assertTrue(dempsy[i].waitToBeStopped(baseTimeoutMillis));

         logger.trace("Finished test testSessionExpiredWithFullAppPlusDynamicAdditions");
      }
   }

   private AtomicBoolean forceIOException = new AtomicBoolean(false);
   private CountDownLatch forceIOExceptionLatch = new CountDownLatch(5);
   
   @Test
   public void testRecoverWithIOException() throws Throwable
   {
      // now lets startup the server.
      ZookeeperTestServer server = null;
      ZookeeperSession sessiong = null;
      try
      {
         server = new ZookeeperTestServer();
         server.start();

         final ZookeeperSession session = new ZookeeperSession("127.0.0.1:" + port,5000) {
            @Override
            protected ZooKeeper makeZooKeeperClient(String connectString, int sessionTimeout) throws IOException
            {
               if (forceIOException.get())
               {
                  forceIOExceptionLatch.countDown();
                  throw new IOException("Fake IO Problem.");
               }
               return super.makeZooKeeperClient(connectString, sessionTimeout);
            }
         };
         sessiong = session;
         
         final ClusterId clusterId = new ClusterId(appname,"testRecoverWithIOException");
         final MicroShardUtils u = new MicroShardUtils(clusterId);
         u.mkAllPersistentAppDirs(session, null);
         TestWatcher callback = new TestWatcher(session)
         {
            @Override public void process()
            {
               try { 
                  session.getSubdirs(u.getClusterDir(),this);
                  called.set(true);
               }
               catch (ClusterInfoException cie) { throw new RuntimeException(cie); }
            }
         };
         
         callback.process();
         
         // force the ioexception to happen
         forceIOException.set(true);
         
         ZookeeperTestServer.forceSessionExpiration(session.zkref.get());
         
         // now in the background it should be retrying but hosed.
         assertTrue(forceIOExceptionLatch.await(baseTimeoutMillis * 3, TimeUnit.MILLISECONDS));

         // now the getActiveSlots call should fail since i'm preventing the recovery by throwing IOExceptions
         assertTrue(TestUtils.poll(baseTimeoutMillis, u, new Condition<MicroShardUtils>()
         {
            @Override
            public boolean conditionMet(MicroShardUtils o) throws Throwable {  
               try { session.mkdir(o.getShardsDir() + "/join-1", DirMode.EPHEMERAL); return false; } catch (ClusterInfoException e) { return true; }
            }
         }));
         
         callback.called.set(false); // reset the callbacker ...
         
         // now we should allow the code to proceed.
         forceIOException.set(false);
         
         // wait for the callback
         assertTrue(poll(baseTimeoutMillis,callback,new Condition<TestWatcher>() {  @Override public boolean conditionMet(TestWatcher o) {  return o.called.get(); } }));
         
         // this should eventually recover.
         assertTrue(TestUtils.poll(baseTimeoutMillis, u, new Condition<MicroShardUtils>()
         {
            @Override
            public boolean conditionMet(MicroShardUtils o) throws Throwable {  
               try { o.mkAllPersistentAppDirs(session, null); session.mkdir(o.getShardsDir() + "/join-1", DirMode.EPHEMERAL); return true; } catch (ClusterInfoException e) { return false; }
            }
         }));
         
         session.getSubdirs(u.getShardsDir(),callback);
         
         // And join should work
         // And join should work
         assertTrue(TestUtils.poll(baseTimeoutMillis,u , new Condition<MicroShardUtils>()
         {
            @Override
            public boolean conditionMet(MicroShardUtils o) throws Throwable {  
               try { session.mkdir(o.getShardsDir() + "/join-1", DirMode.EPHEMERAL); return true; } catch (ClusterInfoException e) {  }
               return false;
            }
         }));

      }
      finally
      {
         if (server != null)
            server.shutdown();
         
         if (sessiong != null)
            sessiong.stop();
      }
   }
}
