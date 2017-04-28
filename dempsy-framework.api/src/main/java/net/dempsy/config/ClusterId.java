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

package net.dempsy.config;

/**
 * <p>
 * This class represents the Id of a message processor cluster within a Dempsy application. Cluster Id's are essentially a two level name: application name, cluster name, and they correspond to the
 * {@link ApplicationDefinition}'s applicationName and the {@link Cluster}'s clusterName.
 * </p>
 * 
 * <p>
 * A cluster Id should be unique.
 * </p>
 * 
 * <p>
 * ClusterIds are immutable.
 * </p>
 * 
 * <p>
 * See the User Guide for an explanation of what a 'message processor cluster' is.
 * </p>
 */
public class ClusterId {
    public final String applicationName;
    public final String clusterName;

    @SuppressWarnings("unused")
    private ClusterId() {
        applicationName = null;
        clusterName = null;
    }

    /**
     * Create a cluster Id from the constituent parts.
     * 
     * @param applicationName
     *            is the application name that the cluster identified with this Id is part of.
     * 
     * @param mpClusterName
     *            is the cluster name within the given application that the cluster identified with this Id is part of.
     */
    public ClusterId(final String applicationName, final String clusterName) {
        this.applicationName = applicationName;
        this.clusterName = clusterName;
        if ((applicationName != null && applicationName.contains(":")) || (clusterName != null && clusterName.contains(":")))
            throw new IllegalArgumentException("Neither the application namd not the cluster name can contain a \":\"");
    }

    /**
     * Convenience constructor for copying an existing ClusterId.
     * 
     * @param other
     *            is the cluster id to make a copy of.
     */
    public ClusterId(final ClusterId other) {
        this.applicationName = other.applicationName;
        this.clusterName = other.clusterName;
    }

    /**
     * This is needed for json
     * @param serialized
     */
    public ClusterId(final String serialized) {
        final int colon = serialized.indexOf(':');
        if (colon < 0)
            throw new IllegalArgumentException("Illegal string representation of a " + ClusterId.class.getSimpleName()
                    + ". Doesn't contain both an application and a cluster name.");
        if (colon != serialized.lastIndexOf(':'))
            throw new IllegalArgumentException(
                    "Illegal string representation of a " + ClusterId.class.getSimpleName() + ". Contains multiple colons");
        applicationName = serialized.substring(0, colon);
        clusterName = serialized.substring(colon + 1);
    }

    /**
     * <p>
     * Provide the ClusterId as a path. The form of the string returned is:
     * </p>
     * <br>
     * 
     * @return: "/" + this.getApplicationName() + "/" + this.getMpClusterName()
     */
    public String asPath() {
        return "/" + this.applicationName + "/" + this.clusterName;
    }

    @Override
    public String toString() {
        return this.applicationName + ":" + this.clusterName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((applicationName == null) ? 0 : applicationName.hashCode());
        result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final ClusterId other = (ClusterId) obj;
        if (applicationName == null) {
            if (other.applicationName != null)
                return false;
        } else if (!applicationName.equals(other.applicationName))
            return false;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        return true;
    }
}
