package net.dempsy.threading;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * The Threading model for Dempsy needs to work in close concert with the Transport. The implementation of the DempsyExecutor should be chosen along with the Transport. If, for example, the transport can handle
 * acknowledged delivery of messages then the Executor should be able to apply 'back pressure' but blocking in the submitLimted.
 * </p>
 */
public interface ThreadingModel extends AutoCloseable {

    /**
     * This is an interface that represents a Callable that can be rejected if the ThreadingModel deems it's to be rejected.
     */
    public static interface Rejectable<V> extends Callable<V> {
        public void rejected();
    }

    /**
     * Submit a Callable that is guaranteed to execute. Unlike {@link submitLimted} this method acts like the {@link Callable} was added to an unbounded queue and so should eventually execute.
     */
    public <V> Future<V> submit(Callable<V> r);

    /**
     * This method queues {@link Callable}s that can expire or have some maximum number allowed. 
     * Normal message processing falls into this category since 'shedding' is the standard behavior.
     * 
     * If counting is {@code true} then the ThreadingModel should queue it without counting it against
     * the maximum capacity.
     */
    public <V> Future<V> submitLimited(Rejectable<V> r, boolean count);

    /**
     * Schedule a task to be executed at some time in the future.
     */
    public <V> Future<V> schedule(Callable<V> r, long delay, TimeUnit timeUnit);

    /**
     * start a daemon process using this ThreadingModel. This defaults to using the 
     * newThread call with the runnable and name and starting it. Note, this does NOT 
     * set the thread to a daemon thread.
     */
    public default void runDaemon(final Runnable daemon, final String name) {
        newThread(daemon, name).start();
    }

    /**
     * start a thread to be used with the threading model. Defaults to simply creating
     * a thread using with the runnable and name. Note, this does NOT set the thread
     * to a daemon thread.
     */
    public default Thread newThread(final Runnable runnable, final String name) {
        return new Thread(runnable, name);
    }

    /**
     * How many pending limited tasks are there
     */
    public int getNumberLimitedPending();

    /**
     * Start up the executor
     */
    public ThreadingModel start();

    /**
     * Perform a clean shutdown of the executor
     */
    @Override
    public void close();

    /**
     * This return value may not be valid prior to start().
     * 
     * @return
     */
    public int getNumThreads();

}
