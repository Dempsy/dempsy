package net.dempsy.messages;

import net.dempsy.lifecycle.annotation.Resource;

/**
 * Default {@link MessageResourceManager} implementation that handles messages implementing
 * the {@link Resource} interface. Replication increments the reference count via
 * {@link Resource#reference()}, and disposal decrements it via {@link Resource#close()}.
 * Non-{@link Resource} messages pass through unchanged.
 *
 * @see Resource
 * @see MessageResourceManager
 */
public class ResourceManager implements MessageResourceManager {

    @Override
    public Object replicate(final Object toReplicate) {
        if(Resource.class.isAssignableFrom(toReplicate.getClass())) {
            ((Resource)toReplicate).reference();
        }
        return toReplicate;
    }

    @Override
    public void dispose(final Object message) {
        if(Resource.class.isAssignableFrom(message.getClass())) {
            final Resource resource = ((Resource)message);
            resource.close();
        }
    }
}
