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

package com.nokia.dempsy.router;

import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSessionFactory;
import com.nokia.dempsy.router.Router.ClusterRouter;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestRouterClusterManagement
{
   Router routerFactory = null;
   
   @MessageProcessor
   public static class GoodTestMp
   {
      @MessageHandler
      public void handle(String message) {}
   }
   
   @Before
   public void init() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      app.setRoutingStrategy(new RoutingStrategy()
      {
         
         @Override
         public Outbound createOutbound()
         {
            return new Outbound()
            {
               @SuppressWarnings("serial")
               SlotInformation slotInfo = new SlotInformation(){};
               
               @Override
               public SlotInformation selectSlotForMessageKey(Object messageKey) throws DempsyException
               {
                  return slotInfo;
               }
               
               @Override
               public void resetCluster(MpCluster<ClusterInformation, SlotInformation> cluster) { }
            };
         }
         
         @Override
         public Inbound createInbound() { return null; }
      });
      app.setSerializer(new JavaSerializer<Object>());
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      cd.setMessageProcessorPrototype(new GoodTestMp());
      app.add(cd);
      app.initialize();
      routerFactory = new Router(app);
      routerFactory.setClusterSession(
            new MpClusterSession<ClusterInformation, SlotInformation>()
      {
         @Override
         public MpCluster<ClusterInformation, SlotInformation> getCluster(ClusterId mpClusterId)
         {
            return new MpClusterTestImpl();
         }

         @Override
         public void stop() { }
      });
      routerFactory.initialize();
   }
   
   @Test
   public void testGetRouterNotFound()
   {
      Set<ClusterRouter> router = routerFactory.getRouter(java.lang.String.class);
      Assert.assertNull(router);
      Assert.assertTrue(routerFactory.missingMsgTypes.containsKey(java.lang.String.class));
   }
   
   @Test
   public void testGetRouterFound()
   {
      Set<ClusterRouter> routers = routerFactory.getRouter(java.lang.Exception.class);
      Assert.assertNotNull(routers);
      Assert.assertEquals(false, routerFactory.missingMsgTypes.containsKey(java.lang.Exception.class));
      Set<ClusterRouter> routers1 = routerFactory.getRouter(ClassNotFoundException.class);
      Assert.assertEquals(routers, routers1);
   }
   
   @Test
   public void testChangingClusterInfo() throws Throwable
   {
      // check that the message didn't go through.
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy.xml", "testDempsy/ClusterManager-LocalVmActx.xml",
            "testDempsy/Transport-PassthroughActx.xml", "testDempsy/SimpleMultistageApplicationActx.xml" );
      Dempsy dempsy = (Dempsy)context.getBean("dempsy");
      MpClusterSessionFactory<ClusterInformation, SlotInformation> factory = dempsy.getClusterSessionFactory();
      MpClusterSession<ClusterInformation, SlotInformation> session = factory.createSession();
      MpCluster<ClusterInformation, SlotInformation> ch = session.getCluster(new ClusterId("test-app", "test-cluster1"));
      ch.setClusterData(new DefaultRoutingStrategy.DefaultRouterClusterInfo(20,2));
      session.stop();
   }
   

}
