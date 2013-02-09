package com.nokia.dempsy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;
import com.nokia.dempsy.router.Router;

@Ignore
public class TestUtils
{
//   private static Logger logger = LoggerFactory.getLogger(TestUtils.class);
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
      boolean finalLoopCondition = false;
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !(finalLoopCondition = condition.conditionMet(userObject));)
         Thread.sleep(10);
//      boolean returnCondition = condition.conditionMet(userObject);
//      if (finalLoopCondition == true && returnCondition == false)
//         logger.error("Return Condition Reversal for " + SafeString.objectDescription(condition));
//      return returnCondition;
      return finalLoopCondition;
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
   
//   public static boolean waitForAllClustersToBeBalanced(long timeoutMillis, ClassPathXmlApplicationContext[] contexts) throws Throwable
//   {
//      // first, separate out all nodes by cluster.
//      Map<ClusterId,List<Dempsy.Application.Cluster.Node>> nodes = getNodes(contexts);
//
//      // flatten them into a big list
//      List<Dempsy.Application.Cluster.Node> flattenedNodes = new ArrayList<Dempsy.Application.Cluster.Node>();
//      for (List<Dempsy.Application.Cluster.Node> cnodes : nodes.values())
//         for (Dempsy.Application.Cluster.Node node : cnodes)
//            flattenedNodes.add(node);
//
//      // make sure they all start.
//      if (!poll(timeoutMillis, flattenedNodes,
//            new Condition<List<Dempsy.Application.Cluster.Node>>()
//            {
//               @Override
//               public boolean conditionMet(List<Dempsy.Application.Cluster.Node> allnodes) 
//               {
//                  for (Dempsy.Application.Cluster.Node node : allnodes)
//                     if (!node.yspmeDteg().isRunning())
//                        return false;
//                  return true;
//               }
//            }))
//         return false;
//
//      // now make sure each cluster group is balanced.
//      for (List<Dempsy.Application.Cluster.Node> cnodes : nodes.values())
//      {
//         // if the first node in the list doesn't contain a strategy then it's an adaptor and we can skip it
//         if (cnodes.get(0).strategyInbound != null)
//         {
//            
//         }
//      }
//   }

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
    * 
    * <p>This method defers to the strategyInbound to see if the cluster is initialized. In 
    * the case of the {@link DecentralizedRoutingStrategy} that means none of the nodes in a 
    * cluster will be considered initialized until they all are. The {@link DecentralizedRoutingStrategy}
    * checks to make sure that all of the shards are claimed before any returns true to the
    * isInitialized call. That means that you need to start at least min_num_nodes_per_clusterParam,
    * which is defaulted to 3, before waitForClustersToBeInitialized will return for any.
    */
   public static boolean waitForClustersToBeInitialized(long timeoutMillis, Dempsy dempsy) throws Throwable
   {
      // wait for it to be running
      if (!poll(timeoutMillis, dempsy,new Condition<Dempsy>() 
         {
            @Override public String toString() { return "Condition(is Dempsy Running)"; }
            @Override public boolean conditionMet(Dempsy dempsy) { return dempsy.isRunning(); } 
         }))
         return false;
      
      return poll(timeoutMillis, dempsy, new Condition<Dempsy>()
      {
         @Override public String toString() { return "Condition(are RoutingStrategy.Inbounds initialized)"; }

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
   
   public static Map<ClusterId,List<Dempsy.Application.Cluster.Node>> getNodes(final ClassPathXmlApplicationContext[] contexts)
   {
      final Map<ClusterId,List<Dempsy.Application.Cluster.Node>> nodes = new HashMap<ClusterId,List<Dempsy.Application.Cluster.Node>>(); 
      for (ClassPathXmlApplicationContext context : contexts)
      {
         Dempsy dempsy = context.getBean(Dempsy.class);
         for (Dempsy.Application app : dempsy.applications.values())
         {
            for (Dempsy.Application.Cluster cluster : app.appClusters)
            {
               ClusterId clusterId = cluster.clusterDefinition.getClusterId();
               List<Dempsy.Application.Cluster.Node> cnodes = nodes.get(clusterId);
               if (cnodes == null)
               {
                  cnodes = new ArrayList<Dempsy.Application.Cluster.Node>();
                  nodes.put(clusterId, cnodes);
               }
               cnodes.addAll(cluster.getNodes());
            }
         }
      }
      return nodes;
   }
   
   public static List<Dempsy.Application.Cluster.Node> getNodes(final ClassPathXmlApplicationContext[] contexts, String clusterName)
   {
      final List<Dempsy.Application.Cluster.Node> nodes = new ArrayList<Dempsy.Application.Cluster.Node>(); 
      for (ClassPathXmlApplicationContext context : contexts)
      {
         Dempsy dempsy = context.getBean(Dempsy.class);
         for (Dempsy.Application app : dempsy.applications.values())
         {
            for (Dempsy.Application.Cluster cluster : app.appClusters)
            {
               if (clusterName.equals(cluster.clusterDefinition.getClusterId().getMpClusterName()))
                  nodes.addAll(cluster.getNodes());
            }
         }
      }
      return nodes;
   }
   
   /**
    * This wait's until all shards are correctly balanced between all nodes and that the expectedNumNodes
    * are available.
    */
   public static boolean waitForClusterBalance(final long timeoutMillis, final String clusterName, 
         final ClassPathXmlApplicationContext[] pcontexts, final int totalShards,
         final int expectedNumNodes, final ClassPathXmlApplicationContext... additionalContexts) throws Throwable
   {
      final int minNumberOfShards = (int)Math.floor((double)totalShards/(double)expectedNumNodes);
      final int maxNumberOfShards = (int)Math.ceil((double)totalShards/(double)expectedNumNodes);
      
      // find all of the nodes that correspond to the given cluster id.
      
      // wait until all nodes have between min and max inclusive.
      List<ClassPathXmlApplicationContext> contexts = new ArrayList<ClassPathXmlApplicationContext>(pcontexts.length + additionalContexts.length);
      contexts.addAll(Arrays.asList(pcontexts));
      contexts.addAll(Arrays.asList(additionalContexts));
      return poll(timeoutMillis, contexts, new Condition<List<ClassPathXmlApplicationContext>>()
      {
         @Override
         public boolean conditionMet(List<ClassPathXmlApplicationContext> contexts) throws Throwable
         {
            List<Dempsy.Application.Cluster.Node> nodes = getNodes(contexts.toArray(new ClassPathXmlApplicationContext[0]),clusterName);
            int totalChecked = 0;
            for (Dempsy.Application.Cluster.Node node : nodes)
            {
               if (node.yspmeDteg().isRunning()) // only consider running nodes.
               {
                  int numShardsCovered =  ((DecentralizedRoutingStrategy.Inbound)node.strategyInbound).getNumShardsCovered();
                  totalChecked++;
                  if (numShardsCovered > maxNumberOfShards || numShardsCovered < minNumberOfShards)
                     return false;
               }
            }
            return totalChecked == expectedNumNodes;
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
   
   public static Dempsy getDempsy(int dempsyIndex, ClassPathXmlApplicationContext[] contexts)
   {
      return contexts[dempsyIndex].getBean(Dempsy.class);
   }
   
   public static Dempsy.Application.Cluster.Node getNode(int dempsyIndex, ClassPathXmlApplicationContext[] contexts)
   {
      Dempsy dempsy = getDempsy(dempsyIndex,contexts);
      if (dempsy.applications.size() != 1) throw new RuntimeException("Expected exactly 1 application but there are " + dempsy.applications.size());
      Dempsy.Application app = dempsy.applications.entrySet().iterator().next().getValue();
      if (app.appClusters.size() != 1) throw new RuntimeException("Expected exactly 1 cluster but there are " + app.appClusters.size());
      Dempsy.Application.Cluster c = app.appClusters.get(0);
      if (c.getNodes().size() != 1) throw new RuntimeException("Expected exactly 1 node but there are " + c.getNodes().size());
      return c.getNodes().get(0);
   }
   
   public static Adaptor getAdaptor(int dempsyIndex, ClassPathXmlApplicationContext[] contexts)
   {
      return getNode(dempsyIndex,contexts).clusterDefinition.getAdaptor();
   }

   public static Object getMp(int dempsyIndex, ClassPathXmlApplicationContext[] contexts)
   {
      return getNode(dempsyIndex,contexts).clusterDefinition.getMessageProcessorPrototype();
   }
   
   public static MpContainer getContainer(int dempsyIndex, ClassPathXmlApplicationContext[] contexts)
   {
      return getNode(dempsyIndex,contexts).getMpContainer();
   }
   
   public static Receiver getReceiver(int dempsyIndex, ClassPathXmlApplicationContext[] contexts) 
   {
      return getNode(dempsyIndex,contexts).getReceiver();
   }

}
