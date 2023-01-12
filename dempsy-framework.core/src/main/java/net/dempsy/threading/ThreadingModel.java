package net.dempsy.threading;

import java.util.Map;
import java.util.concurrent.Callable;

import net.dempsy.Infrastructure;
import net.dempsy.container.MessageDeliveryJob;

/**
 * <p>
 * The Threading model for Dempsy needs to work in close concert with the Transport.
 * The implementation of the DempsyExecutor should be chosen along with the Transport.
 * If, for example, the transport can handle acknowledged delivery of messages then
 * the Executor should be able to apply 'back pressure' by blocking in the submitLimted.
 * </p>
 */
public interface ThreadingModel extends AutoCloseable {

    /**
     * Submit a Callable that is guaranteed to execute. Unlike {@link #submitLimited(Rejectable, boolean)} this method acts
     * like the {@link Callable} was added to an unbounded queue and so should eventually execute.
     */
    public void submit(MessageDeliveryJob r);

    /**
     * This method queues {@link Callable}s that can expire or have some maximum number allowed.
     * Normal message processing falls into this category since 'shedding' is the standard behavior.
     */
    public void submitLimited(MessageDeliveryJob r);

    /**
     * queue a message that will be handled before other non-priority jobs.
     */
    public void submitPrioity(MessageDeliveryJob r);

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
     * Start up the executor.
     */
    public ThreadingModel start(String nodeId);

    /**
     * Perform a clean shutdown of the executor
     */
    @Override
    public void close();

    public boolean isStarted();

    /**
     * Helper method for reading constructed {@link Infrastructure} configuration values.
     */
    public default String getConfigValue(final Map<String, String> conf, final String suffix, final String defaultValue) {
        final String entireKey = configKey(suffix);
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    /**
     * Helper method for reading constructed {@link Infrastructure} configuration values.
     */
    public default String configKey(final String suffix) {
        return this.getClass().getPackageName() + "." + suffix;
    }

}
