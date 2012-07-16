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

package com.nokia.dempsy.cluster;

import java.util.Collection;

public interface ClusterInfoSession
{
   /**
    * This will create a node at the given path. A node must be created before
    * it can be used. This method is not recursive so parent directories will
    * need to be created before this one is created.
    * 
    * @param path a '/' separated path to a directory in the cluster information
    * manager.
    * 
    * @return directory path if the directory was created. Null if the directory 
    * cannot be created or already exists. 
    * 
    * @throws ClusterInfoException on an error which can include the fact that the
    * parent directory doesn't exist.
    */
   public String mkdir(String path, DirMode mode) throws ClusterInfoException;
   
   /**
    * This will remove the directory stored at the path. The directory can be
    * deleted if there is a data object stored in it but it cannot be deleted
    * it it has children. They should be removed first.
    * 
    * @param path is the directory to delete.
    * @return true if the directory is successfully removed and false if it
    * doesn't exist.
    * @throws ClusterInfoException if there is no node at the given path.
    */
   public void rmdir(String path) throws ClusterInfoException;
   
   /**
    * Check if a path has already been created. If the path exists, and if watcher is non-null
    * then the watcher will be called back whenever the node at the path has data added to it, or
    * is deleted.
    * 
    * @param path is the directory to check
    * 
    * @param watcher if non-null, and the path exists, then the watcher will be called 
    * back whenever the node at the path has data added to it, or is deleted.
    * 
    * @return true if the node exists, false otherwise.
    * 
    * @throws ClusterInfoException if there is an unforseem problem.
    */
   public boolean exists(String path, ClusterInfoWatcher watcher) throws ClusterInfoException;
   
   /**
    * If data is stored at the node indicated by the path, then the data will be returned.
    * If the node e
    * @param path
    * @param watcher
    * @return the Object stored at the location.
    * @throws ClusterInfoException
    */
   public Object getData(String path, ClusterInfoWatcher watcher) throws ClusterInfoException;
   
   public void setData(String path, Object data) throws ClusterInfoException;
   
   public Collection<String> getSubdirs(String path, ClusterInfoWatcher watcher) throws ClusterInfoException; 
   
   /**
    * stop() must be implemented such that it doesn't throw an exception no matter what
    * but forces the stopping of any underlying resources that require stopping. Stop
    * is expected to manage the stopping of all underlying MpClusters that it created
    * and once complete no more MpWatcher callbacks should be, or be able to, execute.
    * 
    * NOTE: stop() must be idempotent.
    */
   public void stop();

}
