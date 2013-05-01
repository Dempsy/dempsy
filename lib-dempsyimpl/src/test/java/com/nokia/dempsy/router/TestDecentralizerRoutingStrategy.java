package com.nokia.dempsy.router;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.nokia.dempsy.Dempsy;
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
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestDecentralizerRoutingStrategy
{
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

         inbound = (DecentralizedRoutingStrategy.Inbound)strategy.createInbound(session,clusterId, 
               new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
               destination,new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener()
               {

                  @Override
                  public void keyspaceResponsibilityChanged(final Inbound inbound, boolean less, boolean more)
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
                              catch (Throwable th) { th.printStackTrace(); }
                           }
                        }).start();

                        while (!running.get()) { Thread.yield(); } // wait for the thread to start
                     }
                  }
               });

         inbound.process();

         new Thread(new Runnable()
         {

            @Override
            public void run()
            {
               try
               {
                  while (running.get())
                     session.mkdir(clusterId.asPath()+ "/0", null, DirMode.EPHEMERAL);
               }
               catch (Throwable th) { th.printStackTrace(); }
            }
         }).start();

         for (int i = 0; i < 1000000; i++)
         {
            ((DisruptibleSession)session).disrupt();
            Thread.yield();
         }
      }
      finally
      {
         if (onodes != null)
            System.setProperty("min_nodes_for_cluster", onodes);
         if (oslots != null)
            System.setProperty("total_slots_for_cluster", oslots);
         running.set(false);
         
         if (inbound != null)
            inbound.stop();
         if (tsession != null)
            tsession.stop();
      }
   }
}
