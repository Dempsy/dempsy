package net.dempsy.lifecycle.annotation;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SafeString;

public abstract class AbstractResource implements Resource {
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractResource.class);

    // instantiation is considered a ref count since this is a closable.
    private final transient AtomicLong refCount = new AtomicLong(1);

    // this is for testing
    public long refCount() {
        return refCount.get();
    }

    @Override
    public void init() {
        refCount.set(1);
    }

    @Override
    public void finalize() throws Throwable {
        if(refCount.get() != 0) {
            LOGGER.warn("Resource {} was cleaned up by the VM but doesn't have a 0 refcount ({})", this, refCount);
        }

        super.finalize();
    }

    @Override
    public final void close() {
        final long count = refCount.decrementAndGet();
        if(count < 0)
            LOGGER.warn("The message {} has a negative ref count {}", SafeString.objectDescription(this), count);
        if(count == 0)
            freeResources();
    }

    // called from ResourceManager
    @Override
    public void reference() {
        refCount.incrementAndGet();
    }

    public abstract void freeResources();

}
