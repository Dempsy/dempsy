package com.nokia.dempsy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.router.ClusterInformation;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;
import com.nokia.dempsy.router.SlotInformation;

@Ignore
public class TestUtils
{
   public static interface Condition<T>
   {
      public boolean conditionMet(T o);
   }

   public static <T> boolean poll(long timeoutMillis, T userObject, Condition<T> condition) throws InterruptedException
   {
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !condition.conditionMet(userObject);)
         Thread.sleep(1);
      return condition.conditionMet(userObject);
   }
   
   public static boolean waitForClustersToBeInitialized(long timeoutMillis, 
         final int numSlotsPerCluster, Dempsy dempsy) throws InterruptedException
   {
      try
      {
         final List<ClusterId> clusters = new ArrayList<ClusterId>();
         
         // find out all of the ClusterIds
         for (Dempsy.Application app : dempsy.applications)
         {
            for (Dempsy.Application.Cluster cluster : app.appClusters)
               if (!cluster.clusterDefinition.isRouteAdaptorType())
                  clusters.add(new ClusterId(cluster.clusterDefinition.getClusterId()));
         }
         
         MpClusterSession<ClusterInformation, SlotInformation> session = dempsy.clusterSessionFactory.createSession();
         boolean ret = poll(timeoutMillis, session, new Condition<MpClusterSession<ClusterInformation, SlotInformation>>()
         {
            @Override
            public boolean conditionMet(MpClusterSession<ClusterInformation, SlotInformation> session)
            {
               try
               {
                  for (ClusterId c : clusters)
                  {
                     MpCluster<ClusterInformation, SlotInformation> cluster = session.getCluster(c);
                     Collection<MpClusterSlot<SlotInformation>> slots = cluster.getActiveSlots();
                     if (slots == null || slots.size() != numSlotsPerCluster)
                        return false;
                  }
               }
               catch(MpClusterException e)
               {
                  return false;
               }

               return DecentralizedRoutingStrategy.allOutboundsInitialized();
            }
         });
         
         session.stop();
         return ret;
      }
      catch (MpClusterException e)
      {
         return false;
      }
   }
}
