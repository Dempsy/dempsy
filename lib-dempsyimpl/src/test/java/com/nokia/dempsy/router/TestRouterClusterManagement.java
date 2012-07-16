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

import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.invm.LocalClusterSessionFactory;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.Router.ClusterRouter;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestRouterClusterManagement
{
   Router routerFactory = null;
   RoutingStrategy.Inbound inbound = null;
   
   @MessageProcessor
   public static class GoodTestMp
   {
      @MessageHandler
      public void handle(Exception message) {}
   }
   
   @Before
   public void init() throws Throwable
   {
      final ClusterId clusterId = new ClusterId("test", "test-slot");
      Destination destination = new Destination() {};
      ApplicationDefinition app = new ApplicationDefinition(clusterId.getApplicationName());
      DecentralizedRoutingStrategy strategy = new DecentralizedRoutingStrategy(1, 1);
      app.setRoutingStrategy(strategy);
      app.setSerializer(new JavaSerializer<Object>());
      ClusterDefinition cd = new ClusterDefinition(clusterId.getMpClusterName());
      cd.setMessageProcessorPrototype(new GoodTestMp());
      app.add(cd);
      app.initialize();
      
      LocalClusterSessionFactory mpfactory = new LocalClusterSessionFactory();
      ClusterInfoSession session = mpfactory.createSession();
      
      TestUtils.createClusterLevel(clusterId, session);

      // fake the inbound side setup
      inbound = strategy.createInbound(session,clusterId, 
            new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
         destination);
      
      routerFactory = new Router(app);
      routerFactory.setClusterSession(session);
      routerFactory.initialize();
   }
   
   @After
   public void stop() throws Throwable
   {
      routerFactory.stop();
      inbound.stop();
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
            "testDempsy/Dempsy.xml", "testDempsy/ClusterInfo-LocalActx.xml",
            "testDempsy/Transport-PassthroughActx.xml", "testDempsy/SimpleMultistageApplicationActx.xml" );
      Dempsy dempsy = (Dempsy)context.getBean("dempsy");
      ClusterInfoSessionFactory factory = dempsy.getClusterSessionFactory();
      ClusterInfoSession session = factory.createSession();
      ClusterId curCluster = new ClusterId("test-app", "test-cluster1");
      TestUtils.createClusterLevel(curCluster, session);
      session.setData(curCluster.asPath(), new DecentralizedRoutingStrategy.DefaultRouterClusterInfo(20,2));
      session.stop();
      dempsy.stop();
   }
   

}
