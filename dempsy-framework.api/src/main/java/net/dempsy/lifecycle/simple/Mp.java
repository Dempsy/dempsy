package net.dempsy.lifecycle.simple;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;

public interface Mp {
    public KeyedMessageWithType[] handle(KeyedMessage message);

    public default boolean shouldBeEvicted() {
        return false;
    }

    public default KeyedMessageWithType[] output() {
        return null;
    }

    public default byte[] passivate() {
        return null;
    }

    public default void activate(final byte[] data, final Object key) {}
}
