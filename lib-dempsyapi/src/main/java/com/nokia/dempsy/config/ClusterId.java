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

package com.nokia.dempsy.config;


/**
 * <p>This class represents the Id of a message processor cluster within a Dempsy
 * application. Cluster Id's are essentially a two level name: application name,
 * cluster name, and they correspond to the {@link ApplicationDefinition}'s 
 * applicationName and the {@link ClusterDefinition}'s clusterName.</p>
 * 
 * <p>A cluster Id should be unique.</p>
 * 
 * <p>ClusterIds are immutable.</p>
 * 
 * <p>See the User Guide for an explanation of what a 'message processor cluster' is.</p>
 */
public class ClusterId
{
   private String applicationName;
   private String mpClusterName;
   
   /**
    * Create a cluster Id from the constituent parts. 
    * 
    * @param applicationName is the application name that the cluster identified with 
    * this Id is part of.
    * 
    * @param mpClusterName is the cluster name within the given application that the 
    * cluster identified with this Id is part of.
    */
   public ClusterId(String applicationName, String mpClusterName)
   {
      this.applicationName = applicationName;
      this.mpClusterName = mpClusterName;
   }
   
   /**
    * Convenience constructor for copying an existing ClusterId.
    * 
    * @param other is the cluster id to make a copy of.
    */
   public ClusterId(ClusterId other)
   {
      this.applicationName = other.applicationName;
      this.mpClusterName = other.mpClusterName;
   }

   public String getApplicationName() { return applicationName; }

   public String getMpClusterName() { return mpClusterName; }
   
   /**
    * <p>Provide the ClusterId as a path. The form of the string returned is:</p>
    * <br>
    * 
    * @return: "/" + this.getApplicationName() + "/" + this.getMpClusterName()
    */
   public String asPath() { return "/" + this.getApplicationName() + "/" + this.getMpClusterName(); }
   
   @Override
   public String toString()
   {
      return this.applicationName+":"+this.mpClusterName;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((applicationName == null) ? 0 : applicationName.hashCode());
      result = prime * result + ((mpClusterName == null) ? 0 : mpClusterName.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if(this == obj)
         return true;
      if(obj == null)
         return false;
      if(getClass() != obj.getClass())
         return false;
      ClusterId other = (ClusterId)obj;
      if(applicationName == null)
      {
         if(other.applicationName != null)
            return false;
      }
      else if(!applicationName.equals(other.applicationName))
         return false;
      if(mpClusterName == null)
      {
         if(other.mpClusterName != null)
            return false;
      }
      else if(!mpClusterName.equals(other.mpClusterName))
         return false;
      return true;
   }
}
