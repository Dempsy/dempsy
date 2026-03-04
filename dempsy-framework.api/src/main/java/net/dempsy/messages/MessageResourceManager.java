package net.dempsy.messages;

/**
 * Manages the lifecycle of message objects that carry disposable resources. The framework
 * calls {@link #dispose(Object)} when a message is no longer needed and
 * {@link #replicate(Object)} when a message must be shared across multiple consumers.
 *
 * <p>Implementations must be thread-safe as these methods may be called from multiple
 * threads simultaneously.</p>
 *
 * @see ResourceManager
 * @see DummyMessageResourceManager
 */
public interface MessageResourceManager {

    public void dispose(Object message);

    public Object replicate(Object toReplicate);
}
