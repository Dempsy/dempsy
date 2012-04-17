/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nokia.dempsy.mpcluster;

import java.util.Collection;

import com.nokia.dempsy.config.ClusterId;

/**
 * <p>This interface represents a means of accessing the current state of a cluster.</p>
 * 
 * <p>An Mp cluster is the set of Mp containers that all have the same Mp prototype in them.
 * The collection of all instances of an Mp container that contain the same Mp prototype 
 * is considered an "Mp cluster."</p>
 * 
 * <p>Therefore an instance of an MpCluster represents a handle to the meta-data for 
 * an entire distributed set of Mp prototypes.</p>
 * 
 * <p>This handle can be used to access "slots." It is not necessarily the case that there 
 * is a one to one relationship between a "slot" and a node on a network containing an instance
 * of an Mp container but it is possible. That choice is dependent on the MpCluster implementation
 * and its use within Dempsy.
 * 
 * See the ZookeeperCluster for an example where this isn't the case.
 */
public interface MpCluster<T, N>
{
   /**
    * <p>This will retrieve all of the current slots in a cluster. The collection returned contains 
    * accessors for the state information stored for each slot.</p>
    * 
    * <p>The caller shouldn't hold onto the list as it is not updated as the state
    * of the cluster changes. It is possible that the state can change between
    * the time this call is made and the time a MpClusterSlot is used, so that time
    * should be kept short and access through a MpClusterSlot to a no-longer 
    * existing node needs to be handled by the user.</p>
    */
   public Collection<MpClusterSlot<N>> getActiveSlots() throws MpClusterException;
   
   /**
    * Join the cluster creating a 'slot' out of this instance of the MpCluster implementation.
    * 
    * @return the MpClusterSlot associated with the join instance's state. 
    * Null if no slot was acquired. 
    * 
    * @exception MpClusterException
    */
   public MpClusterSlot<N> join(String slotName) throws MpClusterException;
   
   /**
    * Every MpCluster instance participating in a cluster will have the 
    * same cluster Id, which identifies the total set of Mps of the same prototype.
    */
   public ClusterId getClusterId() throws MpClusterException;
   
   /**
    * Sets the cluster level data.
    * 
    * @param data
    * @throws MpClusterException
    */
   public void setClusterData(T data) throws MpClusterException;
   
   /**
    * returns cluster level data.
    * @return
    * @throws MpClusterException
    */
   public T getClusterData() throws MpClusterException;
   
   /**
    * Add watcher to be invoked during callback. Must be implemented to handle duplicates
    * using Set semantics.
    * @param watch
    */
   public void addWatcher(MpClusterWatcher<T,N> watch);
   
}
