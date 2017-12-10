package net.dempsy.messages;

import java.util.Arrays;

import net.dempsy.config.Cluster;

/**
 * <p>
 * This is a wrapper for a message, along with its key and the types that message represents.
 * </p>
 * 
 * <p>
 * A Dempsy {@link Cluster} is the collection of all Message Processors that handle the same 'message type.'
 * You can think of the 'message type' almost as the address of a {@link Cluster} and the message key as the
 * address of a particular message processor instance within that {@link Cluster}.
 * </p>
 * 
 * @see Cluster
 */
public class KeyedMessageWithType extends KeyedMessage {
    public final String[] messageTypes;

    public KeyedMessageWithType(final Object key, final Object message, final String... messageTypes) {
        super(key, message);
        this.messageTypes = messageTypes;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("KeyedMessageWithType [key=").append(key)
                .append(", message=").append(message)
                .append(", messageTypes=").append(Arrays.toString(messageTypes))
                .append("]").toString();
    }
}
