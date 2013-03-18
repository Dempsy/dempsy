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

import java.util.regex.Pattern;

import com.nokia.dempsy.config.ClusterId;

/**
 * <p>This checks if the cluster is part of a given static cluster. This is used
 * as the default distributed implementation where the instance is initialized
 * with information from the command line using system properties.</p>
 * 
 * <p>You can pass a regular expression as the clusterName portion of the cluster Id.
 * If the regular expression matches the cluster name then isThisNodePartOfCluster
 * will return <code>true</code>.</p>
 */
public class RegExClusterCheck implements CurrentClusterCheck
{
   private ClusterId currentCluster = null;
   private Pattern clusterNamePattern = null;
   
   public RegExClusterCheck(String applicationName, String regExpClusterName)
   {
      this(new ClusterId(applicationName,regExpClusterName));
   }
   
   /**
    * Construct a {@link RegExClusterCheck} that matches the given clusterId. The 
    * clusterName portion of the {@link ClusterId} will be interpreted as a 
    * regular expression to match against the clusterName portion of the check. 
    */
   public RegExClusterCheck(ClusterId clusterId)
   {
      this.currentCluster = clusterId;
      this.clusterNamePattern = Pattern.compile(currentCluster.getMpClusterName());
   }
   
   @Override
   public boolean isThisNodePartOfCluster(ClusterId clusterToCheck)
   {
      if (currentCluster == null)
         return false;

      // assume the clusterName is a regexp. If it's not then this will simply match perfectly.
      return clusterNamePattern.matcher(clusterToCheck.getMpClusterName()).matches();
   }

   @Override
   public boolean isThisNodePartOfApplication(String applicationName)
   {
      String curApplicationName = currentCluster == null ? null : currentCluster.getApplicationName();
      return curApplicationName == null ? false : curApplicationName.equals(applicationName);
   }
}
