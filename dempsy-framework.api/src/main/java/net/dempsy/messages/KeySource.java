/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.messages;

/**
 * An {@link KeySource} is used to provide entire key space to the container
 * so that it can pre-instantiate the MP's that would reside on the node.
 */
public interface KeySource<T> {
    /**
     * This will be called by Dempsy to retrieve all possible keys that that
     * all the nodes in the cluster would ever receive.
     *
     * Container will use only those keys which it would receive based on the
     * cluster configuration.
     *
     * @return
     */
    public Iterable<T> getAllPossibleKeys();
}
