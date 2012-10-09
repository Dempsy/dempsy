package com.nokia.dempsy.cluster;

/**
 * This interface is used in tests. Given an implementation of a {@link ClusterInfoSession} ALSO
 * implements {@link DisruptibleSession}, a test can use 'disrupt' to cause a problem in the 
 * session that will cause it to automatically reset. For example, in the case of Zookeeper it 
 * closes the client.
 */
public interface DisruptibleSession
{
   public void disrupt();
}