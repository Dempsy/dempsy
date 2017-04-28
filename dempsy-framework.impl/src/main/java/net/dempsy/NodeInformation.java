package net.dempsy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import net.dempsy.config.ClusterId;
import net.dempsy.transport.NodeAddress;

public class NodeInformation implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String transportTypeId;
    public final NodeAddress nodeAddress;
    public final Map<ClusterId, ClusterInformation> clusterInfoByClusterId;

    @SuppressWarnings("unused")
    private NodeInformation() {
        transportTypeId = null;
        nodeAddress = null;
        clusterInfoByClusterId = new HashMap<>();
    }

    public NodeInformation(final String transportTypeId, final NodeAddress nodeAddress,
            final Map<ClusterId, ClusterInformation> messageTypesByClusterId) {
        this.transportTypeId = transportTypeId;
        this.nodeAddress = nodeAddress;
        this.clusterInfoByClusterId = new HashMap<>(messageTypesByClusterId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterInfoByClusterId == null) ? 0 : clusterInfoByClusterId.hashCode());
        result = prime * result + ((nodeAddress == null) ? 0 : nodeAddress.hashCode());
        result = prime * result + ((transportTypeId == null) ? 0 : transportTypeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final NodeInformation other = (NodeInformation) obj;
        if (clusterInfoByClusterId == null) {
            if (other.clusterInfoByClusterId != null)
                return false;
        } else if (!clusterInfoByClusterId.equals(other.clusterInfoByClusterId))
            return false;
        if (nodeAddress == null) {
            if (other.nodeAddress != null)
                return false;
        } else if (!nodeAddress.equals(other.nodeAddress))
            return false;
        if (transportTypeId == null) {
            if (other.transportTypeId != null)
                return false;
        } else if (!transportTypeId.equals(other.transportTypeId))
            return false;
        return true;
    }
}