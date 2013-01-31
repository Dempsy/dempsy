package com.nokia.dempsy;

import java.util.Collection;
import java.util.List;

import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.router.Router;

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
    * <p>This allows tests to wait until a Dempsy application is completely up before
    * executing commands. Given the asynchronous nature of relationships between stages
    * of an application, often a test will need to wait until an entire application has
    * been initialized.</p>
    * 
    * <p>In the case of Dynamic Topology the Outbound's are not created until messages 
    * are flowing through and therefore there is still a possibility that the Outbound's
    * wont see this Dempsy instance immediately. Therefore, you cannot use this method
    * and then expect this Dempsy to immediately be the destination for messages
    * sent in some other part of the application.</p>
    */
   public static boolean waitForClustersToBeInitialized(long timeoutMillis, Dempsy dempsy) throws Throwable
   {
      // wait for it to be running
      if (!poll(timeoutMillis, dempsy,new Condition<Dempsy>() { @Override public boolean conditionMet(Dempsy dempsy) { return dempsy.isRunning(); } }))
         return false;
      
      return poll(timeoutMillis, dempsy, new Condition<Dempsy>()
      {
         @Override
         public boolean conditionMet(Dempsy dempsy)
         {
            Collection<Dempsy.Application> apps = dempsy.applications.values();
            if (apps == null || apps.size() == 0)
               return false;
            
            for (Dempsy.Application app : dempsy.applications.values())
            {
               if (app.appClusters == null || app.appClusters.size() == 0)
                  return false;
               
               for (Dempsy.Application.Cluster cl : app.appClusters)
               {
                  if (cl.getNodes() == null || cl.getNodes().size() == 0)
                     return false;
                  
                  for (Dempsy.Application.Cluster.Node cd : cl.getNodes())
                  {
                     if (cd.strategyInbound != null && !cd.strategyInbound.isInitialized())
                        return false;
                  }
               }
            }
            return true;
         }
      });
   }
   
   public static ClusterInfoSession getSession(Dempsy.Application.Cluster c)
   {
      List<Dempsy.Application.Cluster.Node> nodes = c.getNodes(); /// assume one node ... currently a safe assumption, but just in case.
      
      if (nodes == null || nodes.size() != 1)
         throw new RuntimeException("Misconfigured Dempsy application " + SafeString.objectDescription(c));
      
      Dempsy.Application.Cluster.Node node = nodes.get(0);
      
      return node.router.getClusterSession();
   }
   
   public static Dempsy.Application.Cluster.Node getNode(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      return cluster.getNodes().get(0); // currently there is one node per cluster.
   }
   
   public static Object getMp(Dempsy dempsy, String appName, String clusterName)
   {
      return getNode(dempsy,appName,clusterName).clusterDefinition.getMessageProcessorPrototype();
   }

   public static Adaptor getAdaptor(Dempsy dempsy, String appName, String clusterName)
   {
      return getNode(dempsy,appName,clusterName).clusterDefinition.getAdaptor();
   }
   
   public static Router getRouter(Dempsy dempsy, String appName, String clusterName)
   {
      return getNode(dempsy,appName,clusterName).router;
   }

}
