package net.dempsy.messages;

/**
 * No-op {@link MessageResourceManager} that performs no resource cleanup. {@link #dispose(Object)}
 * does nothing and {@link #replicate(Object)} returns the original object. Used when messages
 * do not carry disposable resources.
 */
public class DummyMessageResourceManager implements MessageResourceManager {

    @Override
    public void dispose(final Object message) {}

    @Override
    public Object replicate(final Object toReplicate) {
        return toReplicate;
    }
}
