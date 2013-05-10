package com.nokia.dempsy.router;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.cluster.invm.LocalClusterSessionFactory;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.RoutingStrategy.Inbound;
import com.nokia.dempsy.router.TestRouterClusterManagement.GoodTestMp;
import com.nokia.dempsy.router.microshard.MicroShardUtils;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestDecentralizerRoutingStrategy
{
   private static long baseTimeoutMillis = 20000; // 20 seconds

   @Test
   public void testKeyspaceResponsibilityChangedAccessingInboundAsyncronously() throws Throwable
   {
      String onodes = System.setProperty("min_nodes_for_cluster", "1");
      String oslots = System.setProperty("total_slots_for_cluster", "20");

      final AtomicBoolean running = new AtomicBoolean(false);
      DecentralizedRoutingStrategy.Inbound inbound = null;
      ClusterInfoSession tsession = null;
      
      try
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
         final ClusterInfoSession session = mpfactory.createSession();
         tsession = session;
         
         final AtomicBoolean failed = new AtomicBoolean(false);
         final AtomicBoolean exited = new AtomicBoolean(false);

         inbound = (DecentralizedRoutingStrategy.Inbound)strategy.createInbound(session,clusterId, 
               new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
               destination,new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener()
               {
                  RoutingStrategy.Inbound inbound = null;
                  
                  @Override
                  public void setInboundStrategy(Inbound inbound) { this.inbound = inbound; }

                  @Override
                  public void keyspaceResponsibilityChanged(boolean less, boolean more)
                  {
                     if (!running.get())
                     {
                        new Thread(new Runnable()
                        {

                           @Override
                           public void run()
                           {
                              running.set(true);
                              try
                              {
                                 while (running.get())
                                 {
                                    inbound.doesMessageKeyBelongToNode("Hello");
                                 }
                              }
                              catch (Throwable th) { th.printStackTrace(); failed.set(true); }
                              
                              exited.set(true);
                           }
                        }).start();

                        while (!running.get()) { Thread.yield(); } // wait for the thread to start
                     }
                  }

               });

         inbound.shardChangeWatcher.process();

         Thread shardCreateThread = new Thread(new Runnable()
         {

            @Override
            public void run()
            {
               try
               {
                  MicroShardUtils msutils = new MicroShardUtils(clusterId);
                  while (running.get())
                     session.mkdir(msutils.getShardsDir() + "/0", DirMode.EPHEMERAL);
               }
               catch (Throwable th) { th.printStackTrace(); failed.set(true); }
            }
         });
         shardCreateThread.start();

         for (int i = 0; i < 100; i++)
         {
            ((DisruptibleSession)session).disrupt(100);
            Thread.sleep(1);
         }
         
         running.set(false);
         
         assertTrue(TestUtils.poll(20000, shardCreateThread, new TestUtils.Condition<Thread>()
         {
            @Override public boolean conditionMet(Thread o){ return !o.isAlive() && exited.get(); }
         }));
         
         assertFalse(failed.get());
      }
      finally
      {
         running.set(false);
         
         if (inbound != null)
            inbound.stop();
         if (tsession != null)
            tsession.stop();

         if (onodes != null)
            System.setProperty("min_nodes_for_cluster", onodes);
         else
            System.clearProperty("min_nodes_for_cluster");
         if (oslots != null)
            System.setProperty("total_slots_for_cluster", oslots);
         else
            System.clearProperty("total_slots_for_cluster");
      }
   }

   @Test
   public void testGrabbedSlotFailedPut() throws Throwable
   {
      String onodes = System.setProperty("min_nodes_for_cluster", "1");
      String oslots = System.setProperty("total_slots_for_cluster", "1");

      final AtomicBoolean running = new AtomicBoolean(true);
      DecentralizedRoutingStrategy.Inbound inbound = null;
      ClusterInfoSession tsession = null;
      try
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
         
         final AtomicBoolean thrown = new AtomicBoolean(false);

         LocalClusterSessionFactory mpfactory = new LocalClusterSessionFactory()
         {
            @Override
            public ClusterInfoSession createSession()
            {
               synchronized (currentSessions)
               {
                  LocalSession ret = new LocalSession()
                  {
                     int callCount = 0;
                     
                     @Override
                     synchronized public void setData(String path, Object data) throws ClusterInfoException
                     {
                        callCount++;
                        if (callCount == 1)
                        {
                           thrown.set(true);
                           throw new ClusterInfoException("fake");
                        }
                        else
                           super.setData(path, data);
                     }
                  };
                  currentSessions.add(ret);
                  return ret;
               }
            }
         };
         
         final ClusterInfoSession session = mpfactory.createSession();
         tsession = session;
         
         inbound = (DecentralizedRoutingStrategy.Inbound)strategy.createInbound(session,clusterId, 
               new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
               destination,new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener()
               {
                  @Override
                  public void keyspaceResponsibilityChanged(boolean less, boolean more){ }

                  @Override
                  public void setInboundStrategy(Inbound inbound) {}
               });

         inbound.shardChangeWatcher.process();
         
         assertTrue(TestUtils.poll(baseTimeoutMillis, inbound, new TestUtils.Condition<DecentralizedRoutingStrategy.Inbound>()
         {
            @Override
            public boolean conditionMet(DecentralizedRoutingStrategy.Inbound o) throws Throwable
            {
               return o.doesMessageKeyBelongToNode("Hello");
            }
         }));
         
         assertTrue(thrown.get());

      }
      finally
      {
         running.set(false);
         
         if (inbound != null)
            inbound.stop();
         if (tsession != null)
            tsession.stop();

         if (onodes != null)
            System.setProperty("min_nodes_for_cluster", onodes);
         else
            System.clearProperty("min_nodes_for_cluster");
         if (oslots != null)
            System.setProperty("total_slots_for_cluster", oslots);
         else
            System.clearProperty("total_slots_for_cluster");
      }
   }


}
