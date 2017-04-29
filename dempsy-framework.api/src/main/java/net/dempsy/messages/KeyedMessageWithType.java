package net.dempsy.messages;

public class KeyedMessageWithType extends KeyedMessage {
    public final String[] messageTypes;

    public KeyedMessageWithType(final Object key, final Object message, final String... messageTypes) {
        super(key, message);
        this.messageTypes = messageTypes;
    }
}
