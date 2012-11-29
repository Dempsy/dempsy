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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.TestUtils;

@Ignore
public class ZookeeperTestServer
{
   private static Logger logger = LoggerFactory.getLogger(ZookeeperTestServer.class);
   private File zkDir = null;
   private Properties zkConfig = null;
   private TestZookeeperServerIntern zkServer = null;
   private ClassPathXmlApplicationContext applicationContext = null;
   public static int port = -1;
   
   public static int findNextPort() throws IOException
   {
      // find an unused ehpemeral port
      InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(), 0);
      ServerSocket serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(true); // this allows the server port to be bound to even if it's in TIME_WAIT
      serverSocket.bind(inetSocketAddress);
      port = serverSocket.getLocalPort();
      serverSocket.close();
      return port;
   }

   /**
    * cause a problem with the server running lets sever the connection
    * according to the zookeeper faq we can force a session expired to 
    * occur by closing the session from another client.
    * see: http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
    */
   public static ZooKeeper createExpireSessionClient(ZooKeeper origZk) throws Throwable
   {
      TestUtils.Condition<ZooKeeper> condition = new TestUtils.Condition<ZooKeeper>() 
      {
         @Override public boolean conditionMet(ZooKeeper o) throws Throwable 
         {
            try
            {
               return (o.getState() == ZooKeeper.States.CONNECTED) && o.exists("/", true) != null;
            }
            catch (KeeperException ke)
            {
               return false;
            }
         }
      };
      
      assertTrue(TestUtils.poll(5000, origZk, condition));
      
      long sessionid = origZk.getSessionId();
      byte[] pw = origZk.getSessionPasswd();
      ZooKeeper killer = new ZooKeeper("127.0.0.1:" + port,5000, new Watcher() { @Override public void process(WatchedEvent arg0) { } }, sessionid, pw);
      assertTrue(TestUtils.poll(5000, killer, condition));
      return killer;
   }
   
   public static class InitZookeeperServerBean
   {
      ZookeeperTestServer server = null;
      
      public InitZookeeperServerBean() throws IOException
      {
         ZookeeperTestServer.findNextPort();
         
         server = new ZookeeperTestServer();
         server.start();
         System.setProperty("zk_connect", "127.0.0.1:" + port);
      }
      
      public void stop()
      {
         server.shutdown();
      }
   }
   
   static class TestZookeeperServerIntern extends ZooKeeperServerMain
   {
      @Override
      public void shutdown()
      {
         logger.debug("Stopping internal ZooKeeper server.");
         super.shutdown(); 
      }
   }
   
   public void start() throws IOException
   {
      start(true);
   }
   
   public void start(boolean newDataDir) throws IOException
   {
      // if this is a restart we want to use the same directory
      if (zkDir == null || newDataDir)
         zkDir = genZookeeperDataDir();
      zkConfig = genZookeeperConfig(zkDir);
      port = Integer.valueOf(zkConfig.getProperty("clientPort"));
      zkServer = startZookeeper(zkConfig);
   }
   
   public void shutdown()
   {
      shutdown(true);
   }
   
   public void shutdown(boolean deleteDataDir)
   {
      if (zkServer != null)
      {
         try { zkServer.shutdown(); } catch (Throwable th) 
         {
            logger.error("Failed to shutdown the internal Zookeeper server:",th);
         }
      }

      if (zkDir != null && deleteDataDir)
         deleteRecursivly(zkDir);
   }
   
   public ApplicationContext getApplicationContext() { return applicationContext; }
   
   private static File genZookeeperDataDir()
   {
      File zkDir = null;
      try {
         zkDir = File.createTempFile("zoo", "data");
         if (! zkDir.delete())
            throw new IOException("Can't rm zkDir " + zkDir.getCanonicalPath());
         if (! zkDir.mkdir())
            throw new IOException("Can't mkdir zkDir " + zkDir.getCanonicalPath());
      }catch(IOException e){
         fail("Can't make zookeeper data dir");
      }
      return zkDir;
   }
   
   private static Properties genZookeeperConfig(File zkDir) throws IOException
   {
      Properties props = new Properties();
      props.setProperty("timeTick", "2000");
      props.setProperty("initLimit", "10");
      props.setProperty("syncLimit", "5");
      try {
         props.setProperty("dataDir", zkDir.getCanonicalPath());
      }catch(IOException e){
         fail("Can't create zkConfig, zkDir has no path");
      }
      
      props.setProperty("clientPort", String.valueOf(port));
      return props;
   }
   
   private static TestZookeeperServerIntern startZookeeper(Properties zkConfig)
   {
      logger.debug("Starting the test zookeeper server on port " + zkConfig.get("clientPort"));

      final TestZookeeperServerIntern server = new TestZookeeperServerIntern();
      try {
         QuorumPeerConfig qpConfig = new QuorumPeerConfig();
         qpConfig.parseProperties(zkConfig);
         final ServerConfig sConfig = new ServerConfig();
         sConfig.readFrom(qpConfig);
         
         Thread t = new Thread(new Runnable() {
         
            @Override
            public void run()  {  try { server.runFromConfig(sConfig); } catch(IOException ioe ) { logger.error(MarkerFactory.getMarker("FATAL"), "", ioe); fail("can't start zookeeper"); } }
         });
         t.start();
         Thread.sleep(2000); // give the server time to start
      }catch(Exception e){
         logger.error("Can't start zookeeper", e);
         fail("Can't start zookeeper");
      }
      return server;
   }
   

   private static void deleteRecursivly(File path)
   {
      if (path.isDirectory()) 
         for (File f: path.listFiles())
            deleteRecursivly(f);
      
      logger.debug("Deleting zookeeper data directory:" + path);
      path.delete();
   }
   

}
