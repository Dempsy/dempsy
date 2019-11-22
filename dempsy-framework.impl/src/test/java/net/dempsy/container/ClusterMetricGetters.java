package net.dempsy.container;

/**
 * This interface is to allow the getting of metrics within the tests.
 */
public interface ClusterMetricGetters {
    long getProcessedMessageCount();

    long getDispatchedMessageCount();

    long getMessageFailedCount();

    long getMessageCollisionCount();

    long getMessageDiscardedCount();

    int getInFlightMessageCount();

    double getPreInstantiationDuration();

    double getOutputInvokeDuration();

    double getEvictionDuration();

    long getMessageProcessorsCreated();

    long getMessageProcessorCount();
}
