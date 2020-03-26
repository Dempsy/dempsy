package net.dempsy.messages;

import java.util.List;
import java.util.Set;

import net.dempsy.DempsyException;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;

/**
 * @param <T> is the type of the message processor being managed.
 */
public interface MessageProcessorLifecycle<T> {

    /**
     * This lifecycle phase should be implemented to return a new instance of the message processor. It will
     * be invoked by Dempsy when it needs a fresh instance. This happens, for eample, when a message is dispatched
     * to a cluster with a key that the framework hasn't seen before.
     */
    public T newInstance() throws DempsyException;

    /**
     * The 'activation' lifecycle phase is invoked by Dempsy after a new instance is instantiated
     * (see {@link MessageProcessorLifecycle#newInstance()}) and before the message handling is invoked to give the
     * message processor a chance to prepare.
     */
    public void activate(T instance, Object key) throws DempsyException;

    /**
     * The 'passivation' lifecycle phase is invoked by Dempsy just prior to the framework giving up
     * control of the message processor. This is done when a message processor is being evicted or in
     * the case of elasticity where it's not managed in the current {@link Node} anymore.
     */
    public void passivate(T instance) throws DempsyException;

    /**
     * This method is invoked by the framework when a message is ready to be processed by its target
     * message processor 'instance'.
     */
    public List<KeyedMessageWithType> invoke(T instance, KeyedMessage message) throws DempsyException;

    /**
     * Some Container implementations can queue messages to an individual Mp and deliver them in bulk.
     * By default the method delivers each message to invoke and returns the accumulated responses. It's
     * possible to have the Mp itself handle the bulk delivery. If you want bulk delivery make sure you select
     * a Container that supports it. Not all of them do. If you choose one that doesn't then
     * this method will never be called.
     */
    public List<KeyedMessageWithType> invokeBulk(final T instance, final List<KeyedMessage> message) throws DempsyException;

    /**
     * If the implementation of this Mp can handle bulk delivery, this method should return true. Otherwise
     * it should return false. This should be Mp specific and NOT simply set based on the implementation
     * of this MessageProcessorLifecycle.
     */
    public boolean isBulkDeliverySupported();

    /**
     * This method is invoked by the framework when output is enabled (see {@link #isOutputSupported()})
     * and it's time to invoke the output phase of the particular message processor 'instance.'
     */
    public List<KeyedMessageWithType> invokeOutput(T instance) throws DempsyException;

    /**
     * This method is invoked by Dempsy to determine whether or not the Mps managed by this
     * {@link MessageProcessorLifecycle}
     * support an output cycle.
     */
    public boolean isOutputSupported();

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     */
    public boolean invokeEvictable(T instance) throws DempsyException;

    /**
     * This method is invoked by Dempsy to determine whether or not the Mps managed by this
     * {@link MessageProcessorLifecycle}
     * support being notified of evictions.
     */
    public boolean isEvictionSupported();

    /**
     * This method is invoked by Dempsy to determine what message types are handled by the processors managed
     * by this {@link MessageProcessorLifecycle}.
     *
     * @return the list of message types handled.
     */
    public Set<String> messagesTypesHandled();

    /**
     * The implementor can put validation checks here. This method will be invoked by Dempsy when the cluster
     * is started to check for a valid state. This method should NOT be overloaded to intercept the startup.
     * please see {@link MessageProcessorLifecycle#start(ClusterId)}
     *
     * @throws IllegalStateException when the {@link MessageProcessorLifecycle} is configured incorrectly
     */
    public void validate() throws IllegalStateException;

    /**
     * This method should be implemented to handle carrout out the actual 'start' message processor
     * lifecycle phase. Please see the User Guild for more information on the message processor
     * lifecycle.
     */
    public void start(ClusterId myCluster);

    /**
     * <p>
     * If the messages are resources then this method can be overloaded to manage disposing
     * of the resource. It is guaranteed to be called on all messages passed to the invoke.
     * </p>
     *
     * <p>
     * It will also be called on messages returned from the invoke IFF the transport is a termination
     * for these instances or they never get sent on. For example, the Nio Transport will serialize
     * the messages but then return them to be disposed of while the BlockingQueue transport will
     * queue the very instances to the next Mps. In this later case the transport isn't the
     * termination point for the instances as the very instances will be used in the next
     * stage of Mp processing as a parameter to invoke. Only when they are finally terminated
     * will a dispose be called and only at that termination Mp.
     * </p>
     *
     * <p>
     * Note: THIS METHOD MUST BE THREAD SAFE AS IT WILL BE CALLED FROM MULTIPLE THREADS POTENTIALLY
     * SIMULTANEOUSLY.
     * </p>
     */
    public MessageResourceManager manager();
}
