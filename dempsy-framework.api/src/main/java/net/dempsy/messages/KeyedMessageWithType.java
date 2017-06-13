package net.dempsy.messages;

import java.util.Arrays;

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
