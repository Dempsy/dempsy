package com.nokia.dempsy.mpcluster;

import java.util.Collection;

/**
 * This is a reference to a point in the cluster information store hierarchy where 
 * application specific information can be obtained. See {@link MpCluster} for a description
 * of the structure of the information hierarchy. 
 */
public interface MpApplication<T,N>
{
   /**
    * @return all of the active clusters known about for this {@link MpApplication}
    */
   public Collection<MpCluster<T,N>> getActiveClusters() throws MpClusterException;

   /**
    * Add {@link MpClusterWatcher} to be invoked when the application sees a new cluster.
    * Or a Cluster leaves the Application. The implementer should use using Set semantics
    * to keep track of the {@link MpClusterWatcher}s.
    *  
    * @param watch is the callback to be notified of a change.
    */
   public void addWatcher(MpClusterWatcher watch);
   
}
