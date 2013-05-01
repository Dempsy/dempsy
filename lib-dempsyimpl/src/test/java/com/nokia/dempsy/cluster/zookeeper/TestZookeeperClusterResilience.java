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

import static com.nokia.dempsy.TestUtils.createClusterLevel;
import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.tcp.TcpTransport;
import com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;
import com.nokia.dempsy.router.SpecificClusterCheck;
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
      ZookeeperTestServer server = new ZookeeperTestServer();
      ZookeeperSession session = null;
      final ClusterId clusterId = new ClusterId(appname,"testBouncingServer");
      
      try
      {
         server.start();

         ZookeeperSessionFactory factory = new ZookeeperSessionFactory("127.0.0.1:" + port,5000);
         session = (ZookeeperSession)factory.createSession();
         final ZookeeperSession cluster = session;
         createClusterLevel(clusterId, session);
         TestWatcher callback = new TestWatcher(cluster)
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
                     if (session.getSubdirs(clusterId.asPath(), this).size() == 0)
                        session.mkdir(clusterId.asPath() + "/slot1",null,DirMode.EPHEMERAL);
                     called.set(true);
                  }
                  catch(ClusterInfoException.NoNodeException e)
                  {
                     try
                     {
                        createClusterLevel(clusterId,session);
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

         cluster.exists(clusterId.asPath(), callback);
         callback.process();
         
         // create another session and look
         ZookeeperSession session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new ClusterId(appname,"testBouncingServer").asPath(), null).size());
         session2.stop();

         // kill the server.
         server.shutdown(false);

         // reset the flags
         callback.called.set(false);

         // restart the server
         server.start(false);
         
         // wait for the call
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new TestUtils.Condition<TestWatcher>()
         {
            @Override
            public boolean conditionMet(TestWatcher o) { return o.called.get(); }
         }));

         // get the view from a new session.
         session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new ClusterId(appname,"testBouncingServer").asPath(), null).size());
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
   public void testBouncingServerWithCleanDataDir() throws Throwable
   {
      ZookeeperTestServer server = new ZookeeperTestServer();
      ZookeeperSession session = null;
      final ClusterId clusterId = new ClusterId(appname,"testBouncingServerWithCleanDataDir");
      
      try
      {
         server.start();

         ZookeeperSessionFactory factory = new ZookeeperSessionFactory("127.0.0.1:" + port,5000);
         session = (ZookeeperSession)factory.createSession();
         final ZookeeperSession cluster = session;
         createClusterLevel(clusterId, session);
         TestWatcher callback = new TestWatcher(cluster)
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
                     if (session.getSubdirs(clusterId.asPath(), this).size() == 0)
                        session.mkdir(clusterId.asPath() + "/slot1",null,DirMode.EPHEMERAL);
                     called.set(true);
                  }
                  catch(ClusterInfoException.NoNodeException e)
                  {
                     try
                     {
                        createClusterLevel(clusterId,session);
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

         cluster.exists(clusterId.asPath(), callback);
         callback.process();
         
         // create another session and look
         ZookeeperSession session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new ClusterId(appname,"testBouncingServerWithCleanDataDir").asPath(), null).size());
         session2.stop();

         // kill the server.
         server.shutdown(true);

         // reset the flags
         callback.called.set(false);

         // restart the server
         server.start(true);
         
         // wait for the call
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new TestUtils.Condition<TestWatcher>()
         {
            @Override
            public boolean conditionMet(TestWatcher o) { return o.called.get(); }
         }));

         // get the view from a new session.
         session2 = (ZookeeperSession)factory.createSession();
         assertEquals(1,session2.getSubdirs(new ClusterId(appname,"testBouncingServerWithCleanDataDir").asPath(), null).size());
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
      ZookeeperSession session = (ZookeeperSession)factory.createSession();
      
      ClusterId clusterId = new ClusterId(appname,"testNoServerOnStartup");
      
      // hook a test watch to make sure that callbacks work correctly
      TestWatcher callback = new TestWatcher(session)
      {
         @Override public void process() { called.set(true); }
      };
      
      // now accessing the cluster should get us an error.
      boolean gotCorrectError = false;
      try { session.getSubdirs(clusterId.asPath(), callback); } catch (ClusterInfoException e) { gotCorrectError = true; }
      assertTrue(gotCorrectError);
      
      // now lets startup the server.
      ZookeeperTestServer server = null;
      try
      {
         server = new ZookeeperTestServer();
         server.start();
         
         // create a cluster from the session
         TestUtils.createClusterLevel(clusterId,session);
         
         // wait until this works.
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new Condition<TestWatcher>() {
            @Override public boolean conditionMet(TestWatcher o){  return o.called.get(); }
         }));
         
         callback.called.set(false); // reset the callbacker ...
         
         // now see if the cluster works.
         assertTrue(TestUtils.poll(baseTimeoutMillis, callback, new Condition<TestWatcher>() {
            @Override public boolean conditionMet(TestWatcher o){  return !o.called.get(); }
         }));

         session.getSubdirs(clusterId.asPath(), callback);
         
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
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { session.getSubdirs(clusterId.asPath(), callback); gotCorrectError = false; } catch (ClusterInfoException e) {  }
         }

         session.getSubdirs(clusterId.asPath(), callback);

         // And join should work
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { session.mkdir(clusterId.asPath() + "/join-1", null, DirMode.EPHEMERAL); gotCorrectError = false; } catch (ClusterInfoException e) {  }
         }
         
         assertFalse(gotCorrectError);
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

         // the createExpireSessionClient actually results in a Disconnected/SyncConnected rotating events.
         // ... so we need to filter those out since it will result in a callback.
         session =  new ZookeeperSession("127.0.0.1:" + port,5000);

         final ClusterId clusterId = new ClusterId(appname,"testSessionExpired");
         createClusterLevel(clusterId,session);
         TestWatcher callback = new TestWatcher(session)
         {
            @Override
            public void process()
            {
               try
               {
                  called.set(true);
                  logger.trace("process called on TestWatcher.");
                  session.exists(clusterId.asPath(), this);
                  session.getSubdirs(clusterId.asPath(), this);
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
         
         createClusterLevel(clusterId,session);
         assertTrue(session.exists(clusterId.asPath(), callback));
      }
      finally
      {
         if (server != null)
            server.shutdown();
         
         if (session != null)
            session.stop();
      }
   }
   
   private static Dempsy getDempsyFor(ClusterId clusterId, ApplicationDefinition ad) throws Throwable
   {
      //------------------------------------------------------------------------------
      // here is a complete non-spring, non-DI Dempsy instantiation
      //------------------------------------------------------------------------------
      List<ApplicationDefinition> ads = new ArrayList<ApplicationDefinition>();
      ads.add(ad);
      
      Dempsy dempsy = new Dempsy();
      dempsy.setApplicationDefinitions(ads);
      dempsy.setClusterCheck(new SpecificClusterCheck(clusterId));
      dempsy.setDefaultRoutingStrategy(new DecentralizedRoutingStrategy(20, 1));
      dempsy.setDefaultSerializer(new JavaSerializer<Object>());
      dempsy.setDefaultStatsCollectorFactory(new StatsCollectorFactoryCoda());
      dempsy.setDefaultTransport(new TcpTransport());
      //------------------------------------------------------------------------------

      return dempsy;
   }
   
   @Test
   public void testSessionExpiredWithFullApp() throws Throwable
   {
      // now lets startup the server.
      ZookeeperTestServer server = null;
      final AtomicReference<ZookeeperSession> sessionRef = new AtomicReference<ZookeeperSession>();
      ZookeeperSession session = null;
      final AtomicLong processCount = new AtomicLong(0);
      
      Dempsy[] dempsy = new Dempsy[3];
      try
      {
         server = new ZookeeperTestServer();
         server.start();

         session = new ZookeeperSession("127.0.0.1:" + port,5000) {
            @Override
            public WatcherProxy makeWatcherProxy(ClusterInfoWatcher w)
            {
                     processCount.incrementAndGet();
                     return super.makeWatcherProxy(w);
            };
         };
         sessionRef.set(session);

         final FullApplication app = new FullApplication();
         ApplicationDefinition ad = app.getTopology();

         assertEquals(0,processCount.intValue()); // no calls yet

         dempsy[0] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyAdaptor.class.getSimpleName()),ad);
         dempsy[0].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));

         dempsy[1] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyMp.class.getSimpleName()),ad);
         dempsy[1].setClusterSessionFactory(new ZookeeperSessionFactory("127.0.0.1:" + port,5000));

         dempsy[2] = getDempsyFor(new ClusterId(FullApplication.class.getSimpleName(),FullApplication.MyRankMp.class.getSimpleName()),ad);
//         dempsy[2].setClusterSessionFactory(new ZookeeperSessionFactory<ClusterInformation, SlotInformation>("127.0.0.1:" + port,5000));
         
         dempsy[2].setClusterSessionFactory(new ClusterInfoSessionFactory()
         {
            
            @Override
            public ClusterInfoSession createSession() throws ClusterInfoException
            {
               return sessionRef.get();
            }
         });

         // start everything in reverse order
         for (int i = 2; i >= 0; i--)
            dempsy[i].start();
         
         // make sure the final count is incrementing
         long curCount = app.finalMessageCount.get();
         assertTrue(poll(30000,curCount,new Condition<Long>(){

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
         assertTrue(poll(30000,curCount,new Condition<Long>(){

            @Override
            public boolean conditionMet(Long o)
            {
               return app.finalMessageCount.get() > (o + 100L);
            }
            
         }));

      }
      finally
      {
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
         TestUtils.createClusterLevel(clusterId, session);
         TestWatcher callback = new TestWatcher(session)
         {
            @Override public void process()
            {
               try { 
                  session.getSubdirs(clusterId.asPath(),this);
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
         assertTrue(TestUtils.poll(baseTimeoutMillis, clusterId, new Condition<ClusterId>()
         {
            @Override
            public boolean conditionMet(ClusterId o) throws Throwable {
               try { session.mkdir(o.asPath() + "/join-1", null, DirMode.EPHEMERAL); return false; } catch (ClusterInfoException e) { return true; }
            }
         }));
         
         callback.called.set(false); // reset the callbacker ...
         
         // now we should allow the code to proceed.
         forceIOException.set(false);
         
         // wait for the callback
         assertTrue(poll(baseTimeoutMillis,callback,new Condition<TestWatcher>() { @Override public boolean conditionMet(TestWatcher o) { return o.called.get(); } }));
         
         // this should eventually recover.
         assertTrue(TestUtils.poll(baseTimeoutMillis, clusterId, new Condition<ClusterId>()
         {
            @Override
            public boolean conditionMet(ClusterId o) throws Throwable {
               try { TestUtils.createClusterLevel(o, session); session.mkdir(o.asPath() + "/join-1", null, DirMode.EPHEMERAL); return true; } catch (ClusterInfoException e) { return false; }
            }
         }));
         
         session.getSubdirs(clusterId.asPath(),callback);
         
         // And join should work
         // And join should work
         assertTrue(TestUtils.poll(baseTimeoutMillis,clusterId , new Condition<ClusterId>()
         {
            @Override
            public boolean conditionMet(ClusterId o) throws Throwable {
               try { session.mkdir(o.asPath() + "/join-1", null, DirMode.EPHEMERAL); return true; } catch (ClusterInfoException e) { }
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
