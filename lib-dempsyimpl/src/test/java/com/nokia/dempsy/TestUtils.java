package com.nokia.dempsy;

import java.util.List;

import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.monitoring.StatsCollector;

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
         boolean ret = poll(timeoutMillis, dempsy, new Condition<Dempsy>()
         {
            @Override
            public boolean conditionMet(Dempsy dempsy)
            {
               for (Dempsy.Application app : dempsy.applications.values())
               {
                  for (Dempsy.Application.Cluster cl : app.appClusters)
                  {
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
