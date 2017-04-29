package net.dempsy.transport;

import java.io.Serializable;

/**
 * This class represents and opaque handle to message transport implementation specific means of connecting to a destination.
 */
public interface NodeAddress extends Serializable {
    /**
     * This method defaults to calling toString(). Note, the guid should be able
     * to act as a directory name in the ClusterInfoSession.mkdir call. Therefore
     * it shouldn't have any '/'s in it. Also, see: 
     * https://zookeeper.apache.org/doc/r3.4.10/zookeeperProgrammers.html
     * The section on "The ZooKeeper Data Model."
     */
    public default String getGuid() {
        return this.toString();
    }
}
