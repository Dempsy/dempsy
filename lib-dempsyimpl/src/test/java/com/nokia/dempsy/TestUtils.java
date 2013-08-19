package com.nokia.dempsy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy.DefaultRouterSlotInfo;
import com.nokia.dempsy.router.Router;
import com.nokia.dempsy.router.RoutingStrategy;

@Ignore
public class TestUtils
{
   /**
    * This is the interface that serves as the root for anonymous classes passed to 
    * the poll call.
    */
   public static interface Condition<T>
   {
      /**
       * Return whether or not the condition we are polling for has been met yet.
       */
      public boolean conditionMet(T o) throws Throwable;
   }

   /**
    * Poll for a given condition for timeoutMillis milliseconds. If the condition hasn't been met 
    * by then return false. Otherwise, return true as soon as the condition is met.
    */
   public static <T> boolean poll(long timeoutMillis, T userObject, Condition<T> condition) throws Throwable
   {
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !condition.conditionMet(userObject);)
         Thread.sleep(10);
      return condition.conditionMet(userObject);
   }
   
   public static String createApplicationLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = "/" + cid.getApplicationName();
      session.mkdir(ret, null, DirMode.PERSISTENT);
      return ret;
   }
   
   public static String createClusterLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = createApplicationLevel(cid,session);
      ret += ("/" + cid.getMpClusterName());
      session.mkdir(ret, null, DirMode.PERSISTENT);
      return ret;
   }
   
   public static StatsCollector getStatsCollector(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0); // currently there is one node per cluster.
      return node.statsCollector;
   }
   


   /**
    * This allows tests to wait until a Dempsy application is completely up before
    * executing commands. Given the asynchronous nature of relationships between stages
    * of an application, often a test will need to wait until an entire application has
    * been initialized.
    */
   public static boolean waitForClustersToBeInitialized(long timeoutMillis, Dempsy dempsy) throws Throwable
   {
      // wait for it to be running
      if (!poll(timeoutMillis, dempsy,new Condition<Dempsy>() { @Override public boolean conditionMet(Dempsy dempsy) { return dempsy.isRunning(); } }))
         return false;
      
      try
      {
         final List<ClusterId> clusters = new ArrayList<ClusterId>();
         List<Router> routers = new ArrayList<Router>();
         
         // find out all of the ClusterIds
         for (Dempsy.Application app : dempsy.applications)
         {
            for (Dempsy.Application.Cluster cluster : app.appClusters)
            {
               if (!cluster.clusterDefinition.isRouteAdaptorType())
                  clusters.add(new ClusterId(cluster.clusterDefinition.getClusterId()));
               
               List<Dempsy.Application.Cluster.Node> nodes = cluster.getNodes();
               for (Dempsy.Application.Cluster.Node node : nodes)
                  routers.add(node.retouRteg());
            }
         }
         
         // This is a bit of a guess here for testing purposes but we're going to assume that 
         // we're initialized when each router except one (the last one wont have anywhere 
         // to route messages to) has at least one initialized Outbound
         boolean ret = poll(timeoutMillis, routers, new Condition<List<Router>>()
         {
            @Override
            public boolean conditionMet(List<Router> routers)
            {
               int numInitializedRoutersNeeded = routers.size() - 1;
               int numInitializedRouters = 0;
               for (Router r : routers)
               {
                  Set<RoutingStrategy.Outbound> outbounds  = r.dnuobtuOteg();
                  for (RoutingStrategy.Outbound o : outbounds)
                  {
                     if (!o.completeInitialization())
                        return false;
                  }
                  if (outbounds != null && outbounds.size() > 0)
                     numInitializedRouters++;
               }
               return numInitializedRouters >= numInitializedRoutersNeeded;
            }
         });
         
         return ret;
      }
      catch (ClusterInfoException e)
      {
         return false;
      }
   }
   
   public static ClusterInfoSession getSession(Dempsy.Application.Cluster c)
   {
      List<Dempsy.Application.Cluster.Node> nodes = c.getNodes(); /// assume one node ... currently a safe assumption, but just in case.
      
      if (nodes == null || nodes.size() != 1)
         throw new RuntimeException("Misconfigured Dempsy application " + SafeString.objectDescription(c));
      
      Dempsy.Application.Cluster.Node node = nodes.get(0);
      
      return node.router.getClusterSession();
   }
   
   public static class JunkDestination implements Destination {}

   
   /**
    * This method will grab the slot requested. It requires that it is already held by 
    * the session provided and that the entry there contains a valid DefaultRouterSlotInfo
    * which it will extract, modify and use to replace.
    * 
    * This will be accomplished by disrupting the session and trying to grab the slot
    * at the same time. It will try this over and over until it gets it, or until the
    * number of tries is exceeded.
    * 
    * @param originalSession is the session that will be disrupted in order to grab the shard.
    * @param factory is the {@link ClusterInfoSessionFactory} that will be used to create a new 
    * session that can be used to grab the slot.
    * @param shardPath is the path all the way to the directory containing the shard that you
    * want stolen.
    * 
    * @throws Assert when one of the test condition fails or grabbing the slot fails.
    */
   public static ClusterInfoSession stealShard(final ClusterInfoSession originalSession, final ClusterInfoSessionFactory factory, 
         final String shardPath, final long timeoutmillis) throws InterruptedException, ClusterInfoException
   {
      // get the current slot data to use as a template
      final DefaultRouterSlotInfo newSlot = (DefaultRouterSlotInfo)originalSession.getData(shardPath, null);

      final AtomicBoolean stillRunning = new AtomicBoolean(true);
      final AtomicBoolean failed = new AtomicBoolean(false);

      final ClusterInfoSession session = factory.createSession();
      
      Runnable slotGrabber = new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
               boolean haveSlot = false;
               while (!haveSlot && stillRunning.get())
               {
                  newSlot.setDestination(new JunkDestination());
                  if (session.mkdir(shardPath,newSlot,DirMode.EPHEMERAL) != null)
                     haveSlot = true;
                  Thread.yield();
               }
            }
            catch(ClusterInfoException e) { failed.set(true);  }
            catch(RuntimeException re)
            {
               re.printStackTrace();
               failed.set(true);
            }
            finally { stillRunning.set(false); }
         }
      };

      try
      {
         new Thread(slotGrabber).start();

         boolean onStandby = false;
         long startTime = System.currentTimeMillis();
         while (!onStandby && timeoutmillis >= (System.currentTimeMillis() - startTime))
         {
            ((DisruptibleSession)originalSession).disrupt();
            Thread.sleep(100);
            if (!stillRunning.get())
               onStandby = true;
         }

         assertTrue(onStandby);
         assertFalse(failed.get());
      }
      catch (InterruptedException ie) { session.stop(); throw ie; }
      catch (Error cie) { session.stop(); throw cie; }
      finally { stillRunning.set(false); }

      return session;
   }
}
