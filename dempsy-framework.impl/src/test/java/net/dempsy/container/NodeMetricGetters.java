package net.dempsy.container;

/**
 * This interface is to allow the getting of metrics within the tests.
 */
public interface NodeMetricGetters {
    long getDiscardedMessageCount();

    long getMessagesNotSentCount();

    long getMessagesSentCount();

    long getMessagesReceivedCount();

    long getMessageBytesSent();

    long getMessageBytesReceived();

    long getMessagesPending();

    long getMessagesOutPending();
}
