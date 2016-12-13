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

package net.dempsy.router;

import net.dempsy.config.ClusterId;
import net.dempsy.router.CurrentClusterCheck;

/**
 * This checks if the cluster is part of a given static cluster. This is used
 * as the default distributed implementation where the instance is initialized
 * with information from the command line using system properties.
 */
public class SpecificClusterCheck implements CurrentClusterCheck
{
   private ClusterId currentCluster = null;
   
   public SpecificClusterCheck(ClusterId clusterId) { this.currentCluster = clusterId; }
   
   @Override
   public boolean isThisNodePartOfCluster(ClusterId clusterToCheck)
   {
      return currentCluster == null ? false : currentCluster.equals(clusterToCheck);
   }

   @Override
   public boolean isThisNodePartOfApplication(String applicationName)
   {
      String curApplicationName = currentCluster == null ? null : currentCluster.getApplicationName();
      return curApplicationName == null ? false : curApplicationName.equals(applicationName);
   }
}
