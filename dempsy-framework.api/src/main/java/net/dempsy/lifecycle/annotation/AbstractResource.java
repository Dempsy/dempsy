package net.dempsy.lifecycle.annotation;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SafeString;

public abstract class AbstractResource implements Resource {
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractResource.class);

    private final transient AtomicLong refCount = new AtomicLong(1);

    // private static final transient File trackFile = new File("/tmp/track.out");
    //
    // static {
    // trackFile.delete();
    // }
    //
    // // instantiation is considered a ref count since this is a closable.
    // private final List<RefTracking> tracking = new ArrayList<>();
    //
    // {
    // tracking.add(new RefTracking(Dir.INIT));
    // }
    //
    // private static enum Dir {
    // DECREMENT, INCREMENT, INIT, FINALIZED
    // }
    //
    // private class RefTracking {
    // public final RuntimeException rte = new RuntimeException();
    // public final long curCount = refCount.get();
    // public final Dir dir;
    // public final String thread;
    //
    // private RefTracking(final Dir dir) {
    // this.dir = dir;
    // thread = Thread.currentThread().getName();
    // }
    //
    // private void out(final PrintStream ps) {
    // ps.println("" + dir + " in " + thread + " with count of " + curCount + " at ");
    // rte.printStackTrace(ps);
    // }
    // }

    // this is for testing
    public long refCount() {
        return refCount.get();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void finalize() throws Throwable {
        if(refCount.get() != 0) {
            LOGGER.warn("Resource {} was cleaned up by the VM but doesn't have a 0 refcount ({})", this, refCount);
            // synchronized(this) {
            // tracking.add(new RefTracking(Dir.FINALIZED));
            // }
            // synchronized(AbstractResource.class) {
            // try (final OutputStream os = new BufferedOutputStream(new FileOutputStream(trackFile, true));
            // final PrintStream ps = new PrintStream(os);) {
            // ps.println("===============================================");
            // tracking.forEach(cur -> cur.out(ps));
            // ps.println("===============================================");
            // } catch(final IOException ioe) {
            // LOGGER.error("", ioe);
            // }
            // }
        }

        super.finalize();
    }

    @Override
    public final void close() {
        final long count;
        // synchronized(this) {
        count = refCount.decrementAndGet();

        // tracking.add(new RefTracking(Dir.DECREMENT));
        // }
        // if(count < 0) {
        // synchronized(AbstractResource.class) {
        // try (OutputStream os = new BufferedOutputStream(new FileOutputStream(trackFile, true));
        // PrintStream ps = new PrintStream(os);) {
        // ps.println("===============================================");
        // tracking.forEach(cur -> cur.out(ps));
        // ps.println("===============================================");
        // } catch(final IOException ioe) {
        // LOGGER.error("", ioe);
        // }
        // }
        // }

        if(count < 0)
            LOGGER.warn("The message {} has a negative ref count {}", SafeString.valueOfClass(this), count);

        if(count == 0)
            freeResources();
    }

    // called from ResourceManager
    @Override
    public void reference() {
        // synchronized(this) {
        refCount.incrementAndGet();
        // tracking.add(new RefTracking(Dir.INCREMENT));
        // }
    }

    public abstract void freeResources();

}
