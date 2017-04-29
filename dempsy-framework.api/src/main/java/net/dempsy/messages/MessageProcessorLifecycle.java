package net.dempsy.messages;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import net.dempsy.DempsyException;
import net.dempsy.config.ClusterId;

public interface MessageProcessorLifecycle<T> {

    /**
     * Creates a new instance from the prototype.
     */
    public T newInstance() throws DempsyException;

    /**
     * Invokes the activation method of the passed instance.
     */
    public void activate(T instance, Object key, byte[] activationData) throws IllegalArgumentException, DempsyException;

    /**
     * Invokes the passivation method of the passed instance. Will return the object's passivation data, 
     * <code>null</code> if there is none.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public byte[] passivate(T instance) throws IllegalArgumentException, DempsyException;

    /**
     * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing
     * <code>null</code> messages.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public List<KeyedMessageWithType> invoke(T instance, KeyedMessage message) throws IllegalArgumentException, DempsyException;

    /**
     * Invokes the output method, if it exists. If the instance does not have an annotated output method,
     *  this is a no-op (this is simpler than requiring the caller to check every instance).
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public List<KeyedMessageWithType> invokeOutput(T instance) throws DempsyException;

    public boolean isOutputSupported();

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     * 
     * @param instance
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public boolean invokeEvictable(T instance) throws DempsyException;

    public boolean isEvictionSupported();

    public Set<String> messagesTypesHandled();

    public void validate() throws IllegalStateException;

    public void start(ClusterId myCluster);
}
