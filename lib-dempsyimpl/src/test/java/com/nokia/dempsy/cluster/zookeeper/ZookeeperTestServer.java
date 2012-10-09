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

import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Ignore;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

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
      zkDir = genZookeeperDataDir();
      zkConfig = genZookeeperConfig(zkDir);
      port = Integer.valueOf(zkConfig.getProperty("clientPort"));
      zkServer = startZookeeper(zkConfig);
   }
   
   public void shutdown()
   {
      if (zkServer != null)
      {
         try { zkServer.shutdown(); } catch (Throwable th) 
         {
            logger.error("Failed to shutdown the internal Zookeeper server:",th);
         }
      }

      if (zkDir != null)
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
      path.delete();
   }
   

}
