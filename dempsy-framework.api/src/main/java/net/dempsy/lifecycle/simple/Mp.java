package net.dempsy.lifecycle.simple;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;

/**
 * When using the 'simple' message processor lifecycle, this interface will be implemented by the framework user
 * to do the actual message processing. 
 */
public interface Mp {
    public KeyedMessageWithType[] handle(KeyedMessage message);

    public default boolean shouldBeEvicted() {
        return false;
    }

    public default KeyedMessageWithType[] output() {
        return null;
    }

    public default void passivate() {}

    public default void activate(final Object key) {}
}
