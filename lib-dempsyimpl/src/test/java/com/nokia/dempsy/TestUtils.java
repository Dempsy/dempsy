package com.nokia.dempsy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.router.Router;
import com.nokia.dempsy.router.RoutingStrategy;

@Ignore
public class TestUtils
{
   public static interface Condition<T>
   {
      public boolean conditionMet(T o) throws Throwable;
   }

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

   public static boolean waitForClustersToBeInitialized(long timeoutMillis, 
         final int numSlotsPerCluster, Dempsy dempsy) throws Throwable
   {
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
         // were initialized when each router except one (the last one wont have anywhere 
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
}
