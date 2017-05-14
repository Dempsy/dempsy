package net.dempsy.router.group.intern;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.transport.NodeAddress;

public class GroupDetails implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String groupName;
    public final NodeAddress node;
    public final Map<String, Integer> clusterIndicies;
    public ContainerAddress[] containerAddresses;

    @SuppressWarnings("unused") // serialization. Yay!
    private GroupDetails() {
        groupName = null;
        node = null;
        clusterIndicies = null;
        containerAddresses = null;
    }

    public GroupDetails(final String groupName, final NodeAddress nodeAddress) {
        this.groupName = groupName;
        this.node = nodeAddress;
        this.clusterIndicies = new HashMap<>();
        this.containerAddresses = null;
    }

    public void fillout(final Map<String, ContainerAddress> caByCluster) throws IllegalStateException {
        final int size = caByCluster.size();
        this.containerAddresses = new ContainerAddress[size];
        caByCluster.entrySet().forEach(e -> {
            final ContainerAddress ca = e.getValue();
            final int[] indicies = ca.clusters;
            for (final int index : indicies) {
                if (containerAddresses[index] != null)
                    throw new IllegalStateException(
                            "Two different clusters have the same container index (" + index + "). One is " + e.getKey() + ".");
                containerAddresses[index] = ca;
            }
        });

        for (int i = 0; i < containerAddresses.length; i++) {
            if (containerAddresses[i] == null)
                throw new IllegalStateException("Missing container address at index " + i);
        }

        // now set clusterIndicies
        caByCluster.entrySet().forEach(e -> {
            final String cn = e.getKey();
            final ContainerAddress ca = e.getValue();
            IntStream.of(ca.clusters).forEach(i -> {
                final Integer index = Integer.valueOf(i);
                if (clusterIndicies.containsKey(cn)) {
                    if (!clusterIndicies.get(cn).equals(index))
                        throw new IllegalStateException("cluster " + cn + " seems to corespond to multiple clusters in the group including "
                                + clusterIndicies.get(cn) + " and " + i);
                } else
                    clusterIndicies.put(cn, index);
            });
        });
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(containerAddresses);
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
        if (!Arrays.equals(containerAddresses, other.containerAddresses))
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