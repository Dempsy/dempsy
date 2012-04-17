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

import java.util.Collection;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSlot;

public class OtherNode
{
   public static void main(String[] args) throws Throwable
   {
      ZookeeperSessionFactory<String, String> factory = new ZookeeperSessionFactory<String, String>("127.0.0.1:2181",5000);
      MpClusterSession<String, String> session = factory.createSession();
      MpCluster<String, String> cluster = session.getCluster(new ClusterId("testApp","testCluster"));
      cluster.join("otherNode").setSlotInformation("Hello From Another Place");

      while (true)
      {
         try
         {
            Thread.sleep(5000);
            
            Collection<MpClusterSlot<String>> nodes = cluster.getActiveSlots();
            System.out.println("===================================");
            for (MpClusterSlot<String> node : nodes)
               System.out.println("   " + node + ":" + node.getSlotInformation());
         }
         catch (Throwable th)
         {
            th.printStackTrace();
         }
      }
   }
}
