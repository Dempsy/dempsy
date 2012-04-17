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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;

public class TestZookeeperClusterImpl
{
   @Test(expected=MpClusterException.class)
   public void testNoSever() throws Throwable
   {
      // pass in a bogus hostname
      ZookeeperSessionFactory<String, String> factory = new ZookeeperSessionFactory<String, String>("127..0.1:2181",5000);
      factory.createSession();
   }
   
   KeeperException appropriateException = null;
   
   @Test
   public void testBadZooKeeperConnection() throws Throwable
   {
      int port = ZookeeperTestServer.findNextPort();
      ZookeeperTestServer server = new ZookeeperTestServer();
      
      Throwable receivedException = null;
      ZookeeperSession<String, String> session = null;
      
      try
      {
         server.start();
         
         session = new ZookeeperSession<String, String>("127.0.0.1:" + port,5000) {
            
            @Override
            protected ZooKeeper makeZookeeperInstance(String connectString, int sessionTimeout) throws IOException
            {
               return new ZooKeeper(connectString,sessionTimeout,new ZkWatcher())
               {
                  @Override
                  public List<String> getChildren(String path, Watcher watcher) throws KeeperException
                  {
                     throw (appropriateException = new KeeperException(Code.DATAINCONSISTENCY)
                     {
                        private static final long serialVersionUID = 1L;
                     });
                  }
               };
            }
                  
               };

         MpCluster<String, String> cluster = session.getCluster(new ClusterId("test","test"));
         
         try
         {
            cluster.getActiveSlots();
         }
         catch(Exception e)
         {
            receivedException = e.getCause();
         }
      }
      finally
      {
         server.shutdown();
         
         if (session != null)
            session.stop();
      }
      
      assertNotNull(appropriateException);
      assertTrue(receivedException == appropriateException);
   }

}
