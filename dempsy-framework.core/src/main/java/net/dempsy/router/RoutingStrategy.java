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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import net.dempsy.KeyspaceChangeListener;
import net.dempsy.Service;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.transport.NodeAddress;

public interface RoutingStrategy {

    /**
     * A {@link ContainerAddress} represents the address of a message processor's
     * container within a node. In the case where a message is sent to 2 containers
     * within a node (possible when one node hosts both containers, each one part
     * of a different cluster) the {@code clusters} array will contain the index
     * of both containers.
     */
    public static final class ContainerAddress implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * The address of the node housing the container(s)
         */
        public final NodeAddress node;

        /**
         * The set of indices for each Container within the node that a message
         * will be sent to.
         */
        public final int[] clusters;

        @SuppressWarnings("unused") // (de)serialization requirement
        private ContainerAddress() {
            node = null;
            clusters = null;
        }

        public ContainerAddress(final NodeAddress node, final int cluster) {
            this.node = node;
            this.clusters = new int[] { cluster };
        }

        public ContainerAddress(final NodeAddress node, final int[] clusters) {
            this.node = node;
            this.clusters = clusters;
        }

        @Override
        public String toString() {
            return "ContainerAddress[node=" + node + ", clusters=" + Arrays.toString(clusters) + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(clusters);
            result = prime * result + ((node == null) ? 0 : node.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (ContainerAddress.class != obj.getClass()) // can do this for a final class
                return false;
            final ContainerAddress other = (ContainerAddress) obj;
            if (!Arrays.equals(clusters, other.clusters))
                return false;
            if (node == null) {
                if (other.node != null)
                    return false;
            } else if (!node.equals(other.node))
                return false;
            return true;
        }
    }

    public static interface Factory extends Service {
        /**
         * Get the routing strategy associated with the downstream cluster denoted
         * but {@code clusterId}. There should be no need to {@code start()} the 
         * Router that's returned. Since the {@link Factory} is the manager for
         * the {@link Router}s, the caller should {@code release} the {@link Router}
         * when it's done. 
         */
        public Router getStrategy(ClusterId clusterId);

        /**
         * This will be called from the {@link RoutingStrategyManager} to let the {@link Factory}
         * know what the typeId was that created it. The typeId can contain information 
         * useful to the {@link Factory}. By default this method does nothing.
         */
        public default void typeId(final String typeId) {}

    }

    /**
     * The Router's responsibility is to provide the {@link ContainerAddress} of the 
     * destination of the {@link KeyedMessageWithType}. A Router is a cluster level object
     * so the set of destinations visible from a particular instance will be specifically 
     * for the cluster.
     */
    public static interface Router {

        /**
         * Determine the destination for this given message. 
         */
        public ContainerAddress selectDestinationForMessage(KeyedMessageWithType message);

        /**
         * What are the complete set of visible destinations for the cluster who's 
         * {@link ClusterId} was used to retrieve this Router.
         */
        public Collection<ContainerAddress> allDesintations();

        /**
         * This will call release on the {@link Factory} that created it.
         */
        public void release();
    }

    public static interface Inbound extends Service {
        /**
         * This method will be called prior to start to provide context for the operation
         * of the Inbound strategy.
         */
        public void setContainerDetails(ClusterId clusterId, ContainerAddress address, KeyspaceChangeListener listener);

        /**
         * Since the {@link Inbound} has the responsibility to determine which instances of a 
         * MessageProcessors are valid in 'this' node, it should be able to provide that
         * information through the implementation of this method. 
         */
        public boolean doesMessageKeyBelongToNode(Object messageKey);

        /**
         * Provide the routing strategy id for the {@link RoutingStrategyManager} to look 
         * up the {@link Router} from clients of this container.
         */
        public default String routingStrategyTypeId() {
            return this.getClass().getPackage().getName();
        }

        /**
         * This will be called from the RoutingInboundManager to let the {@link Inbound}
         * know what the typeId was that created it. The typeId can contain information 
         * useful to the {@link Inbound}. By default this method does nothing.
         */
        public default void typeId(final String typeId) {}
    }
}
