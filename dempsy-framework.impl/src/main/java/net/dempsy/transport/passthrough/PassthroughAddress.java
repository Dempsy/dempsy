package net.dempsy.transport.passthrough;

import net.dempsy.transport.NodeAddress;

public class PassthroughAddress implements NodeAddress {
    private static final long serialVersionUID = 1L;

    final long destinationId;

    @SuppressWarnings("unused")
    private PassthroughAddress() {
        destinationId = -1;
    }

    PassthroughAddress(final long destinationId) {
        this.destinationId = destinationId;
    }

    @Override
    public String toString() {
        return "Passthrough(" + destinationId + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (destinationId ^ (destinationId >>> 32));
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
        final PassthroughAddress other = (PassthroughAddress) obj;
        if (destinationId != other.destinationId)
            return false;
        return true;
    }
}
