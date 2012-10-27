package com.nokia.dempsy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.monitoring.StatsCollector;
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
         Thread.sleep(1);
      return condition.conditionMet(userObject);
   }
   
   public static String createApplicationLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = "/" + cid.getApplicationName();
      session.mkdir(ret, DirMode.PERSISTENT);
      return ret;
   }
   
   public static String createClusterLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = createApplicationLevel(cid,session);
      ret += ("/" + cid.getMpClusterName());
      session.mkdir(ret, DirMode.PERSISTENT);
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
}
