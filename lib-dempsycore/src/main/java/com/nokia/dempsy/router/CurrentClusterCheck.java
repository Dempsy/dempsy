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

import com.nokia.dempsy.config.ClusterId;

/**
 * <p>Implementations of this interface help to answer the question of whether or not
 * the cluster we're running in is the one identified.</p>
 * 
 * <p>Why would anyone need this? Dempsy can be run distributed (hence the 'D') or it
 * can be run in a single VM (or perhaps other layouts not yet envisioned). However, certain
 * components need to know whether or not they are being instantiated within a node that's part
 * of a particular cluster. This interface provides a flexible way to answer that question.</p>
 *
 */
public interface CurrentClusterCheck
{
   public boolean isThisNodePartOfCluster(ClusterId clusterToCheck);
   
   public boolean isThisNodePartOfApplication(String applicationName);
}
