package net.dempsy.lifecycle.simple;

import java.util.Arrays;
import java.util.List;

import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;

/**
 * When using the 'simple' message processor lifecycle, this interface will be implemented by the framework user
 * to do the actual message processing.
 */
public interface Mp {
    public KeyedMessageWithType[] handle(KeyedMessage message);

    public default KeyedMessageWithType[] handleBulk(final List<KeyedMessage> messages) {
        final KeyedMessageWithType[] ret = messages.stream()
            .map(m -> handle(m))
            .filter(ms -> ms != null)
            .flatMap(ms -> Arrays.stream(ms))
            .toArray(KeyedMessageWithType[]::new);

        return (ret.length > 0) ? ret : null;
    }

    public default boolean shouldBeEvicted() {
        return false;
    }

    public default KeyedMessageWithType[] output() {
        return null;
    }

    public default void passivate() {}

    public default void activate(final Object key, final Object activatingMessage) {}
}
