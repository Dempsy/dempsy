package net.dempsy.transport;

import java.io.Serializable;

public class RoutedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public final int[] containers;
    public final Object key;
    public final Object message;

    @SuppressWarnings("unused")
    private RoutedMessage() {
        containers = null;
        key = null;
        message = null;
    }

    public RoutedMessage(final int[] containers, final Object key, final Object message) {
        this.containers = containers;
        this.key = key;
        this.message = message;
    }
}