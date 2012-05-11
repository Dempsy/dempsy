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

package com.nokia.dempsy.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.KeySource;
import com.nokia.dempsy.annotations.Evictable;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Start;

public class TestConfig
{

   public static class GoodMessage
   {
      @MessageKey
      public String key() { return "Hello"; }
   }
   
   @MessageProcessor
   public static class GoodTestMp
   {
      @MessageHandler
      public void handle(GoodMessage string) {}
      
      @Start
      public void startMethod() {}
      
      @Evictable
      public boolean evict(){return false;}
   }
   
   @MessageProcessor
   public static class MultiStartTestMp
   {
     @MessageHandler
     public void handle(GoodMessage string) {}
     
     @Start
     public void startMethod() {}
     
     @Start
     public void extraStartMethod() {}
     
   }
   
   public static class GoodAdaptor implements Adaptor
   {
      @Override
      public void setDispatcher(Dispatcher dispatcher){ }
      
      @Override
      public void start() {}
      
      @Override
      public void stop() {}

   }

   @Test
   public void testSimpleConfig() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      cd.setMessageProcessorPrototype(new GoodTestMp());
      app.add(cd);
      app.initialize();
      
      System.out.println(app);
      
      // if we get to here without an error we should be okay
      app.validate(); // this throws if there's a problem.
      
      assertNull(cd.getSerializer());
      assertNull(app.getSerializer());

      assertNull(cd.getRoutingStrategy());
      assertNull(app.getRoutingStrategy());
      
      assertNull(cd.getStatsCollectorFactory());
      assertNull(app.getStatsCollectorFactory());
   }
   
   @Test
   public void testConfig() throws Throwable
   {
      List<ClusterDefinition> clusterDefs = new ArrayList<ClusterDefinition>();
      
      Object appSer;
      Object appRs;
      Object appScf;
      ApplicationDefinition app = new ApplicationDefinition("test").
            setSerializer(appSer = new Object()).
            setRoutingStrategy(appRs = new Object())
            .setStatsCollectorFactory(appScf = new Object());

      ClusterDefinition cd = new ClusterDefinition("test-slot1").setAdaptor(new GoodAdaptor());
      clusterDefs.add(cd);
      
      cd = new ClusterDefinition("test-slot2",new GoodTestMp()).
            setDestinations(new ClusterId(new ClusterId("test", "test-slot3")));
      clusterDefs.add(cd);
      
      cd = new ClusterDefinition("test-slot3");
      cd.setMessageProcessorPrototype(new GoodTestMp());
      cd.setDestinations(new ClusterId[] { new ClusterId("test", "test-slot4"), new ClusterId("test", "test-slot5")});
      clusterDefs.add(cd);

      Object clusSer;
      cd = new ClusterDefinition("test-slot4").setMessageProcessorPrototype(new GoodTestMp()).setSerializer(clusSer = new Object());
      clusterDefs.add(cd);

      Object clusRs;
      cd = new ClusterDefinition("test-slot5").setMessageProcessorPrototype(new GoodTestMp()).setRoutingStrategy(clusRs = new Object());
      clusterDefs.add(cd);

      Object clusScf;
      cd = new ClusterDefinition("test-slot6").setMessageProcessorPrototype(new GoodTestMp()).setStatsCollectorFactory(clusScf = new Object());
      clusterDefs.add(cd);
      
      cd = new ClusterDefinition("test-slot1.5", new GoodAdaptor());
      assertNotNull(cd.getAdaptor());
      clusterDefs.add(cd);

      app.setClusterDefinitions(clusterDefs);
      
      app.initialize();
      // if we get to here without an error we should be okay
      app.validate(); // this throws if there's a problem.

      System.out.println(app);
      
      assertTrue(app.getClusterDefinitions().get(0).isRouteAdaptorType());
      assertEquals(new ClusterId("test", "test-slot2"), app.getClusterDefinitions().get(1).getClusterId());
      assertEquals("test",app.getClusterDefinitions().get(1).getClusterId().getApplicationName());
      assertEquals("test-slot2",app.getClusterDefinitions().get(1).getClusterId().getMpClusterName());
      assertEquals(new ClusterId("test", "test-slot2").hashCode(), app.getClusterDefinitions().get(1).getClusterId().hashCode());
      assertFalse(new ClusterId("test", "test-slot3").equals(new Object()));
      assertFalse(new ClusterId("test", "test-slot3").equals(null));
      
      assertEquals(appSer,app.getClusterDefinitions().get(0).getSerializer());
      assertEquals(app.getSerializer(),app.getClusterDefinitions().get(1).getSerializer());
      assertEquals(clusSer,app.getClusterDefinitions().get(3).getSerializer());

      assertEquals(appRs,app.getClusterDefinitions().get(0).getRoutingStrategy());
      assertEquals(app.getRoutingStrategy(),app.getClusterDefinitions().get(1).getRoutingStrategy());
      assertEquals(appRs,app.getClusterDefinitions().get(3).getRoutingStrategy());
      assertEquals(clusRs,app.getClusterDefinitions().get(4).getRoutingStrategy());
      
      assertEquals(appScf, app.getClusterDefinitions().get(0).getStatsCollectorFactory());
      assertEquals(app.getStatsCollectorFactory(), app.getClusterDefinitions().get(1).getStatsCollectorFactory());
      assertEquals(clusScf, app.getClusterDefinitions().get(5).getStatsCollectorFactory());
      
      assertTrue(app == app.getClusterDefinitions().get(4).getParentApplicationDefinition());
      
      assertEquals(new ClusterId("test", "test-slot1"),app.getClusterDefinitions().get(0).getClusterId());
   }
   
   @Test(expected=DempsyException.class)
   public void testFailNoPrototypeOrAdaptor() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot1");
      app.add(cd); // no prototype or adaptor
      System.out.println(app);
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testFailBadPrototype() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot1");
      cd.setMessageProcessorPrototype(new Object()); // has no annotated methods
      app.add(cd);
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testFailBothPrototypeAndAdaptor() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot1");
      cd.setMessageProcessorPrototype(new GoodTestMp());
      cd.setAdaptor(new GoodAdaptor());
      app.add(cd);
      System.out.println(app);
      app.initialize();
   }
   
   @Test(expected=DempsyException.class)
   public void testFailNullClusterDefinition() throws Throwable
   {
      ApplicationDefinition app = 
            new ApplicationDefinition("test").add(
                  new ClusterDefinition("test-slot1").setMessageProcessorPrototype(new GoodTestMp()),
                  null,
                  new ClusterDefinition("test-slot2").setMessageProcessorPrototype(new GoodTestMp()));
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testFailNoParent() throws Throwable
   {
      new ClusterDefinition("test-slot1").setMessageProcessorPrototype(new GoodTestMp()).validate();
   }
   
   @Test(expected=DempsyException.class)
   public void testDupCluster() throws Throwable
   {
      ApplicationDefinition app = 
            new ApplicationDefinition("test-tooMuchWine-needMore").add(
                  new ClusterDefinition("notTheSame").setAdaptor(new GoodAdaptor()),
                  new ClusterDefinition("mp-stage1").setMessageProcessorPrototype(new GoodTestMp()),
                  new ClusterDefinition("mp-stage2-dupped").setMessageProcessorPrototype(new GoodTestMp()),
                  new ClusterDefinition("mp-stage2-dupped").setMessageProcessorPrototype(new GoodTestMp()),
                  new ClusterDefinition("mp-stage3").setMessageProcessorPrototype(new GoodTestMp()));
      app.validate();
   }


   @Test(expected=DempsyException.class)
   public void testMultipleStartMethodsDisallowed() throws Throwable
   {
      ApplicationDefinition app =
         new ApplicationDefinition("test-multiple-starts").add(
               new ClusterDefinition("adaptor").setAdaptor(new GoodAdaptor()),
               new ClusterDefinition("good-mp").setMessageProcessorPrototype(new GoodTestMp()),
               new ClusterDefinition("bad-mp").setMessageProcessorPrototype(new MultiStartTestMp()));
      app.validate();
   }

   @Test
   public void testSimpleConfigWithKeyStore() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      cd.setMessageProcessorPrototype(new GoodTestMp());
      cd.setKeySource(new KeySource<Object>()
      {
         @Override
         public Iterable<Object> getAllPossibleKeys(){ return null; }
      });
      app.add(cd);
      app.initialize();
   }

   @Test
   public void testConfigWithKeyStore() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      cd.setMessageProcessorPrototype(new GoodTestMp())
      .setKeySource(new KeySource<Object>()
      {
         @Override
         public Iterable<Object> getAllPossibleKeys()
         {
            return null;
         }
      });
      app.add(cd);
      app.initialize();
   }
   
   @Test(expected=DempsyException.class)
   public void testConfigAdaptorWithKeyStore() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      cd.setAdaptor(new Adaptor()
      {
         @Override
         public void stop(){ }
         
         @Override
         public void start(){ }
         
         @Override
         public void setDispatcher(Dispatcher dispatcher){ }
      })
      .setKeySource(new KeySource<Object>()
      {
         @Override
         public Iterable<Object> getAllPossibleKeys()
         {
            return null;
         }
      });
      app.add(cd);
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testConfigMpWithMultipleEvict() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      
      @MessageProcessor
      class mp 
      {
         @SuppressWarnings("unused")
         @MessageHandler
         public void handle(GoodMessage string) {}
         
         @SuppressWarnings("unused")
         @Start
         public void startMethod() {}
         
         @SuppressWarnings("unused")
         @Evictable
         public boolean evict2(){return false;}

         @SuppressWarnings("unused")
         @Evictable
         public boolean evict1(){return false;}
         
      }
      
      cd.setMessageProcessorPrototype(new mp());
      app.add(cd);
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testConfigMpWithWrongReturnTypeEvict1() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      
      @MessageProcessor
      class mp 
      {
         @SuppressWarnings("unused")
         @MessageHandler
         public void handle(GoodMessage string) {}
         
         @SuppressWarnings("unused")
         @Start
         public void startMethod() {}
         
         @SuppressWarnings("unused")
         @Evictable
         public void evict1(){ }
         
      }
      
      cd.setMessageProcessorPrototype(new mp());
      app.add(cd);
      app.initialize();
   }

   @Test(expected=DempsyException.class)
   public void testConfigMpWithWrongReturnTypeEvict2() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      
      @MessageProcessor
      class mp 
      {
         @SuppressWarnings("unused")
         @MessageHandler
         public void handle(GoodMessage string) {}
         
         @SuppressWarnings("unused")
         @Start
         public void startMethod() {}
         
         @SuppressWarnings("unused")
         @Evictable
         public Object evict1(){ return null; }
         
      }
      
      cd.setMessageProcessorPrototype(new mp());
      app.add(cd);
      app.initialize();
   }

   @Test
   public void testConfigMpWithGoodMPEvict() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      ClusterDefinition cd = new ClusterDefinition("test-slot");
      
      @MessageProcessor
      class mp 
      {
         @SuppressWarnings("unused")
         @MessageHandler
         public void handle(GoodMessage string) {}
         
         @SuppressWarnings("unused")
         @Start
         public void startMethod() {}
         
         @SuppressWarnings("unused")
         @Evictable
         public boolean evict1(Object arg){ return false; }
         
      }
      
      cd.setMessageProcessorPrototype(new mp());
      app.add(cd);
      app.initialize();
   }

}
