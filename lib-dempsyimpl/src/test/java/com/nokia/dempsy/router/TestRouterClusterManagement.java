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
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.invm.LocalClusterSessionFactory;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollectorFactory;
import com.nokia.dempsy.router.Router.ClusterRouter;
import com.nokia.dempsy.router.RoutingStrategy.Inbound;
import com.nokia.dempsy.router.microshard.MicroShardUtils;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestRouterClusterManagement
{
   Router routerFactory = null;
   RoutingStrategy.Inbound inbound = null;
   
   public static class GoodMessage
   {
      @MessageKey
      public String key() { return "hello"; }
   }
   
   public static class GoodMessageChild extends GoodMessage
   {
   }
   
   
   @MessageProcessor
   public static class GoodTestMp
   {
      @MessageHandler
      public void handle(GoodMessage message) {}
   }
   
   public static class Message
   {
      @MessageKey
      public String key() { return "hello"; }
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
      
      MicroShardUtils utils = new MicroShardUtils(clusterId);
      utils.mkClusterDir(session, null);

      // fake the inbound side setup
      inbound = strategy.createInbound(session,clusterId, 
            new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
         destination,new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener()
         {
            
            @Override
            public void keyspaceResponsibilityChanged(Inbound inbound, boolean less, boolean more) { }
         });
      
      routerFactory = new Router(cd);
      routerFactory.setClusterSession(session);
      routerFactory.setCurrentCluster(clusterId);
      routerFactory.setStatsCollector(new BasicStatsCollectorFactory().createStatsCollector(clusterId, new Destination() {} ));
      routerFactory.start();
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
      // lazy load requires the using of the router.
      routerFactory.dispatch(new Message());
      Set<ClusterRouter> router = routerFactory.getRouter(Message.class);
      Assert.assertNull(router);
      Assert.assertTrue(routerFactory.getMissingTypes().contains(Message.class));
   }
   
   @Test
   public void testGetRouterFound()
   {
      routerFactory.dispatch(new GoodMessage());
      Set<ClusterRouter> routers = routerFactory.getRouter(GoodMessage.class);
      Assert.assertNotNull(routers);
      Assert.assertEquals(false, routerFactory.getMissingTypes().contains(GoodMessage.class));
      Set<ClusterRouter> routers1 = routerFactory.getRouter(GoodMessageChild.class);
      Assert.assertEquals(routers, routers1);
      Assert.assertEquals(new ClusterId("test", "test-slot"), routerFactory.getThisClusterId());
   }
   
   @Test
   public void testChangingClusterInfo() throws Throwable
   {
      // check that the message didn't go through.
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy.xml", "testDempsy/ClusterInfo-LocalActx.xml", "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/Transport-PassthroughActx.xml", "testDempsy/SimpleMultistageApplicationActx.xml" );
      Dempsy dempsy = (Dempsy)context.getBean("dempsy");
      ClusterInfoSessionFactory factory = dempsy.getClusterSessionFactory();
      ClusterInfoSession session = factory.createSession();
      ClusterId curCluster = new ClusterId("test-app", "test-cluster1");
      new MicroShardUtils(curCluster).mkClusterDir(session, new DecentralizedRoutingStrategy.DefaultRouterClusterInfo(20,2,null));
      session.stop();
      dempsy.stop();
   }
}
