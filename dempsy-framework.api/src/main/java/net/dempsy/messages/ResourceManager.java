package net.dempsy.messages;

import net.dempsy.lifecycle.annotation.Resource;

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
