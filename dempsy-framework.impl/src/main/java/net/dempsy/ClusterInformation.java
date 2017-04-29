package net.dempsy;

import java.util.HashSet;
import java.util.Set;

import net.dempsy.config.ClusterId;

public class ClusterInformation {
    public final Set<String> messageTypesHandled;
    public final String routingStrategyTypeId;
    public final ClusterId clusterId;

    @SuppressWarnings("unused")
    private ClusterInformation() {
        this.routingStrategyTypeId = null;
        this.clusterId = null;
        this.messageTypesHandled = null;
    }

    public ClusterInformation(final String routingStrategyTypeId, final ClusterId clusterId, final Set<String> messageTypesHandled) {
        this.routingStrategyTypeId = routingStrategyTypeId;
        this.clusterId = clusterId;
        this.messageTypesHandled = new HashSet<>(messageTypesHandled);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
        result = prime * result + ((messageTypesHandled == null) ? 0 : messageTypesHandled.hashCode());
        result = prime * result + ((routingStrategyTypeId == null) ? 0 : routingStrategyTypeId.hashCode());
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
        final ClusterInformation other = (ClusterInformation) obj;
        if (clusterId == null) {
            if (other.clusterId != null)
                return false;
        } else if (!clusterId.equals(other.clusterId))
            return false;
        if (messageTypesHandled == null) {
            if (other.messageTypesHandled != null)
                return false;
        } else if (!messageTypesHandled.equals(other.messageTypesHandled))
            return false;
        if (routingStrategyTypeId == null) {
            if (other.routingStrategyTypeId != null)
                return false;
        } else if (!routingStrategyTypeId.equals(other.routingStrategyTypeId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ClusterInformation [messageTypesHandled=" + messageTypesHandled + ", routingStrategyTypeId=" + routingStrategyTypeId + ", clusterId="
                + clusterId + "]";
    }
}
