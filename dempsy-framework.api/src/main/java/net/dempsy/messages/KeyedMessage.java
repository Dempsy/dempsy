package net.dempsy.messages;

/**
 * A {@link KeyedMessage} is a simple wrapper around a message, and its corresponding key.
 */
public class KeyedMessage {
    public final Object key;
    public final Object message;

    public KeyedMessage(final Object key, final Object message) {
        this.key = key;
        this.message = message;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("KeyedMessage [key=").append(key)
                .append(", message=").append(message)
                .append("]").toString();
    }
}
