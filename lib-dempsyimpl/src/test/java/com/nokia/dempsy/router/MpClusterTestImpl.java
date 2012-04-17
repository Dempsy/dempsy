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

package com.nokia.dempsy.router;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.junit.Ignore;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

@Ignore
public class MpClusterTestImpl implements MpCluster<ClusterInformation, SlotInformation>
{
   public static class MpClusterSlotTestImpl implements MpClusterSlot<SlotInformation>
   {
      @SuppressWarnings("serial")
      private SlotInformation info = new SlotInformation() {};
      private String slotName;
      
      private MpClusterSlotTestImpl(String slotName)
      {
         this.info.addMessageClass(java.lang.Exception.class);
         this.slotName = slotName;
      }
      
      @Override
      public String getSlotName() { return slotName; }
      
      @Override
      public SlotInformation getSlotInformation()
      {
         // TODO Auto-generated method stub
         return info;
      }

      @Inject
      public void setSlotInformation(SlotInformation info)
      {
         this.info = info;
      }
      
      @Override
      public void leave() { }
   }

	private List<MpClusterSlot<SlotInformation>> nodes = new ArrayList<MpClusterSlot<SlotInformation>>();
	
	private ClusterInformation data = null;
	
   public MpClusterTestImpl() { nodes.add(new MpClusterSlotTestImpl("test")); }
	
	@Override
	public void addWatcher(MpClusterWatcher<ClusterInformation,SlotInformation> watch) { }

	@Override
	public Collection<MpClusterSlot<SlotInformation>> getActiveSlots() { return this.nodes; }

	@Override
	public ClusterInformation getClusterData() { return data; }

	@Override
	public ClusterId getClusterId() {	return null; }

   @Override
	public MpClusterSlot<SlotInformation> join(String nodeName)
	{
	   MpClusterSlotTestImpl node = new MpClusterSlotTestImpl(nodeName);
      this.nodes.add(node);

		return node;
	}

	@Override
	public void setClusterData(ClusterInformation data) { this.data = data; }
	
	public void setNodes(List<MpClusterSlot<SlotInformation>> nodes) { this.nodes = nodes;	}

}
