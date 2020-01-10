package net.dempsy.lifecycle.annotation;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.QuietCloseable;
import net.dempsy.util.SafeString;

public abstract class AbstractResource implements Resource {
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractResource.class);

    private final transient AtomicLong refCount = new AtomicLong(1);

    private static final boolean TRACK_RESOURCE_LIFECYCLE;

    static {
        final String sysOpTRACKMEMLEAKS = System.getProperty("dempsy.TRACK_RESOURCE_LIFECYCLE");
        final boolean sysOpSet = sysOpTRACKMEMLEAKS != null;
        boolean track = ("".equals(sysOpTRACKMEMLEAKS) || Boolean.parseBoolean(sysOpTRACKMEMLEAKS));
        if(!sysOpSet)
            track = Boolean.parseBoolean(System.getenv("DEMPSY_TRACK_RESOURCE_LIFECYCLE"));

        TRACK_RESOURCE_LIFECYCLE = track;
    }

    private static final transient File trackFile = TRACK_RESOURCE_LIFECYCLE ? new File("/tmp/track.out") : null;

    static {
        if(TRACK_RESOURCE_LIFECYCLE)
            trackFile.delete();
    }

    // instantiation is considered a ref count since this is a closable.
    private final List<RefTracking> tracking = TRACK_RESOURCE_LIFECYCLE ? new ArrayList<>() : null;

    {
        if(TRACK_RESOURCE_LIFECYCLE)
            tracking.add(new RefTracking(Dir.INIT));
    }

    // this is for testing
    public long refCount() {
        return refCount.get();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void finalize() throws Throwable {
        if(refCount.get() != 0) {
            LOGGER.warn("Resource {} was cleaned up by the VM but doesn't have a 0 refcount ({})", this, refCount);
            if(TRACK_RESOURCE_LIFECYCLE) {
                synchronized(this) {
                    tracking.add(new RefTracking(Dir.FINALIZED));
                }
                synchronized(AbstractResource.class) {
                    try (final OutputStream os = new BufferedOutputStream(new FileOutputStream(trackFile, true));
                        final PrintStream ps = new PrintStream(os);) {
                        ps.println("===============================================");
                        tracking.forEach(cur -> cur.out(ps));
                        ps.println("===============================================");
                    } catch(final IOException ioe) {
                        LOGGER.error("", ioe);
                    }
                }
            }
        }

        super.finalize();
    }

    @Override
    public final void close() {
        final long count;

        if(TRACK_RESOURCE_LIFECYCLE) {
            synchronized(this) {
                count = refCount.decrementAndGet();

                tracking.add(new RefTracking(Dir.DECREMENT));
            }
            if(count < 0) {
                synchronized(AbstractResource.class) {
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(trackFile, true));
                        PrintStream ps = new PrintStream(os);) {
                        ps.println("===============================================");
                        tracking.forEach(cur -> cur.out(ps));
                        ps.println("===============================================");
                    } catch(final IOException ioe) {
                        LOGGER.error("", ioe);
                    }
                }
            }

            if(count < 0)
                LOGGER.warn("The message {} has a negative ref count {}", SafeString.valueOfClass(this), count);

            if(count == 0)
                freeResources();
        } else {
            count = refCount.decrementAndGet();

            if(count < 0)
                LOGGER.warn("The message {} has a negative ref count {}", SafeString.valueOfClass(this), count);

            if(count == 0)
                freeResources();
        }
    }

    // called from ResourceManager
    @Override
    public void reference() {
        if(TRACK_RESOURCE_LIFECYCLE) {
            synchronized(this) {
                refCount.incrementAndGet();
                tracking.add(new RefTracking(Dir.INCREMENT));
            }
        } else {
            refCount.incrementAndGet();
        }
    }

    public abstract void freeResources();

    /**
     * This should be used only for testing when managing your own resources
     * and unit testing MPs without the framework.
     */
    public static class Closer implements QuietCloseable {
        private final List<AbstractResource> toClose = new ArrayList<>();

        public <T extends AbstractResource> List<T> addAll(final List<T> resources) {
            if(resources != null)
                toClose.addAll(resources);
            return resources;
        }

        public <T extends AbstractResource> T add(final T resource) {
            if(resource != null)
                toClose.add(resource);
            return resource;
        }

        @Override
        public void close() {
            final int sizeM1 = toClose.size() - 1;
            IntStream.range(0, sizeM1 + 1)
                .map(i -> sizeM1 - i)
                .mapToObj(i -> toClose.get(i))
                .filter(r -> r != null)
                .forEach(r -> r.close());
        }

    }

    private static enum Dir {
        DECREMENT, INCREMENT, INIT, FINALIZED
    }

    private class RefTracking {
        public final RuntimeException rte = new RuntimeException();
        public final long curCount = refCount.get();
        public final Dir dir;
        public final String thread;

        private RefTracking(final Dir dir) {
            this.dir = dir;
            thread = Thread.currentThread().getName();
        }

        private void out(final PrintStream ps) {
            ps.println("" + dir + " in " + thread + " with count of " + curCount + " at ");
            rte.printStackTrace(ps);
        }
    }

}
