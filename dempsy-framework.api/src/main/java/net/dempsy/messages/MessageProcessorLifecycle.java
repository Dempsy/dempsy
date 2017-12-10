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
     * This method is invoked by the framework when output is enabled (see {@link #isOutputSupported()})
     * and it's time to invoke the output phase of the particular message processor 'instance.'
     */
    public List<KeyedMessageWithType> invokeOutput(T instance) throws DempsyException;

    /**
     * This method is invoked by Dempsy to determine whether or not the Mps managed by this {@link MessageProcessorLifecycle}
     * support an output cycle.
     */
    public boolean isOutputSupported();

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     */
    public boolean invokeEvictable(T instance) throws DempsyException;

    /**
     * This method is invoked by Dempsy to determine whether or not the Mps managed by this {@link MessageProcessorLifecycle}
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
}
