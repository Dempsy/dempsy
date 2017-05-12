package net.dempsy.router.group.intern;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.transport.NodeAddress;

public class GroupDetails implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String groupName;
    public final NodeAddress node;
    public final Map<String, ContainerAddress> caByCluster;

    @SuppressWarnings("unused") // serialization. Yay!
    private GroupDetails() {
        groupName = null;
        node = null;
        caByCluster = null;
    }

    public GroupDetails(final String groupName, final NodeAddress nodeAddress) {
        this.groupName = groupName;
        this.node = nodeAddress;
        this.caByCluster = new HashMap<>();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((caByCluster == null) ? 0 : caByCluster.hashCode());
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + ((node == null) ? 0 : node.hashCode());
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
        final GroupDetails other = (GroupDetails) obj;
        if (caByCluster == null) {
            if (other.caByCluster != null)
                return false;
        } else if (!caByCluster.equals(other.caByCluster))
            return false;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        } else if (!groupName.equals(other.groupName))
            return false;
        if (node == null) {
            if (other.node != null)
                return false;
        } else if (!node.equals(other.node))
            return false;
        return true;
    }
}