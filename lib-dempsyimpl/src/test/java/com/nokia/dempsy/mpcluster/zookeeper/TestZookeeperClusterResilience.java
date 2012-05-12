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

package com.nokia.dempsy.mpcluster.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

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
   
   public static class TestWatcher implements MpClusterWatcher
   {
      AtomicBoolean called = new AtomicBoolean(false);
      @Override
      public void process()
      {
         called.set(true);
      }
      
   }
   
   @Test
   public void testBouncingServer() throws Throwable
   {
      ZookeeperTestServer server = new ZookeeperTestServer();
      ZookeeperSession<String, String> session = null;
      
      try
      {
         server.start();

         ZookeeperSessionFactory<String, String> factory = new ZookeeperSessionFactory<String, String>("127.0.0.1:" + port,5000);
         session = (ZookeeperSession<String, String>)factory.createSession();
         final MpCluster<String, String> cluster = session.getCluster(new ClusterId(appname,"testBouncingServer"));
         TestWatcher callback = new TestWatcher()
         {
            MpCluster<String, String> m_cluster = cluster;
            
            @Override
            public void process()
            {
               try
               {
                  if (m_cluster.getActiveSlots().size() == 0)
                  {
                     m_cluster.join("slot1");
                     called.set(true);
                  }
               }
               catch(MpClusterException e)
               {
                  // this will fail when the connection is severed... that's ok.
               }
            }

         };

         cluster.addWatcher(callback);
         callback.process();

         // create another session and look
         ZookeeperSession<String, String> session2 = (ZookeeperSession<String, String>)factory.createSession();
         MpCluster<String, String> cluster2 = session2.getCluster(new ClusterId(appname,"testBouncingServer"));
         assertEquals(1,cluster2.getActiveSlots().size());
         session2.stop();

         // kill the server.
         server.shutdown();

         // reset the flags
         callback.called.set(false);

         // restart the server
         server.start();

         // wait for the call
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !callback.called.get();)
            Thread.sleep(1);
         assertTrue(callback.called.get());

         // get the view from a new session.
         session2 = (ZookeeperSession<String, String>)factory.createSession();
         cluster2 = session2.getCluster(new ClusterId(appname,"testBouncingServer"));
         assertEquals(1,cluster2.getActiveSlots().size());
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
      ZookeeperSessionFactory<String, String> factory = new ZookeeperSessionFactory<String, String>("127.0.0.1:" + port,5000);
      ZookeeperSession<String, String> session = (ZookeeperSession<String, String>)factory.createSession();
      MpCluster<String, String> cluster = session.getCluster(new ClusterId(appname,"testNoServerOnStartup"));
      TestWatcher callback = new TestWatcher();
      cluster.addWatcher(callback);
      
      assertNotNull(cluster);
      
      // now accessing the cluster should get us an error.
      boolean gotCorrectError = false;
      try { cluster.getActiveSlots(); } catch (MpClusterException e) { gotCorrectError = true; }
      assertTrue(gotCorrectError);
      
      // now lets startup the server.
      ZookeeperTestServer server = null;
      try
      {
         server = new ZookeeperTestServer();
         server.start();
         
         // wait until this works.
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !callback.called.get();)
            Thread.sleep(1);
         
         assertTrue(callback.called.get());
         callback.called.set(false); // reset the callbacker ...
         
         // now see if the cluster works.
         cluster.getActiveSlots();
         
         // now we should be all happycakes ... but with the server running lets sever the connection
         // according to the zookeeper faq we can force a session expired to occur by closing the session from another client.
         // see: http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
         ZooKeeper origZk = session.zkref.get();
         long sessionid = origZk.getSessionId();
         callback.called.set(false); // reset the callbacker ...
         ZooKeeper killer = new ZooKeeper("127.0.0.1:" + port,5000, new Watcher() { @Override public void process(WatchedEvent arg0) { } }, sessionid, null);
         killer.close(); // tricks the server into expiring the other session
         
         // wait for the callback
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !callback.called.get();)
            Thread.sleep(10);
         assertTrue(callback.called.get());
         
         // unfortunately I cannot check the getActiveSlots for failure because there's a race condition I can't fix.
         //  No matter how fast I check it's possible that it's okay again OR that allSlots hasn't been cleared.
         // 
         // however, they should eventually recover.
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { cluster.getActiveSlots(); gotCorrectError = false; } catch (MpClusterException e) {  }
         }

         cluster.getActiveSlots();

         // And join should work
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { cluster.join("join-1"); gotCorrectError = false; } catch (MpClusterException e) {  }
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
   
   private AtomicBoolean forceIOException = new AtomicBoolean(false);
   private CountDownLatch forceIOExceptionLatch = new CountDownLatch(5);
   
   @Test
   public void testRecoverWithIOException() throws Throwable
   {
      // now lets startup the server.
      ZookeeperTestServer server = null;
      ZookeeperSession<String, String> session = null;
      try
      {
         server = new ZookeeperTestServer();
         server.start();

         session =  new ZookeeperSession<String, String>("127.0.0.1:" + port,5000) {
            @Override
            protected ZooKeeper makeZookeeperInstance(String connectString, int sessionTimeout) throws IOException
            {
               if (forceIOException.get())
               {
                  forceIOExceptionLatch.countDown();
                  throw new IOException("Fake IO Problem.");
               }
               return super.makeZookeeperInstance(connectString, sessionTimeout);
            }
         };
         
         MpCluster<String, String> cluster = session.getCluster(new ClusterId(appname,"testRecoverWithIOException"));
         TestWatcher callback = new TestWatcher();
         cluster.addWatcher(callback);

         assertNotNull(cluster);
         
         // now see if the cluster works.
         cluster.getActiveSlots();
         
         // cause a problem with the server running lets sever the connection
         // according to the zookeeper faq we can force a session expired to occur by closing the session from another client.
         // see: http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
         ZooKeeper origZk = session.zkref.get();
         long sessionid = origZk.getSessionId();
         ZooKeeper killer = new ZooKeeper("127.0.0.1:" + port,5000, new Watcher() { @Override public void process(WatchedEvent arg0) { } }, sessionid, null);
         
         // force the ioexception to happen
         forceIOException.set(true);
         
         killer.close(); // tricks the server into expiring the other session
         
         // just stop the damn server
         server.shutdown();
         
         // now in the background it should be retrying but hosed.
         assertTrue(forceIOExceptionLatch.await(baseTimeoutMillis * 3, TimeUnit.MILLISECONDS));
         
         // wait for the callback
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !callback.called.get();)
            Thread.sleep(1);
         assertTrue(callback.called.get());
         
         // TODO: do I really meed this sleep?
         Thread.sleep(1000);
         
         // now the getActiveSlots call should fail since i'm preventing the recovery by throwing IOExceptions
         boolean gotCorrectError = false;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !gotCorrectError;)
         {
            Thread.sleep(1);
            try { cluster.join("yo"); } catch (MpClusterException e) { gotCorrectError = true; }
         }
         assertTrue(gotCorrectError);
         
         callback.called.set(false); // reset the callbacker ...
         
         // now we should allow the code to proceed.
         forceIOException.set(false);
         
         // we might want the server running.
         server = new ZookeeperTestServer();
         server.start();

         // wait for the callback
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && !callback.called.get();)
            Thread.sleep(1);
         assertTrue(callback.called.get());
         
         // this should eventually recover.
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { cluster.getActiveSlots(); gotCorrectError = false; } catch (MpClusterException e) {  }
         }
         
         cluster.getActiveSlots();
         
         // And join should work
         gotCorrectError = true;
         for (long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis() && gotCorrectError;)
         {
            Thread.sleep(1);
            try { cluster.join("join-1"); gotCorrectError = false; } catch (MpClusterException e) {  }
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

}
