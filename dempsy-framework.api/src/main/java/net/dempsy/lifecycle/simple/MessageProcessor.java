package net.dempsy.lifecycle.simple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import net.dempsy.DempsyException;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;

/**
 * <p>
 * {@link MessageProcessor} is a simple implementation of a {@link MessageProcessorLifecycle}  where the user
 * can define their specific message processors by implementing the {@link Mp} interface. This technique
 * ties the specific implementation of the message processor more tightly to the framework than using the
 * annotation driven approach but may be slightly more efficient.
 * </p>
 * <p>
 * To use this class you would instantiate it as the {@link MessageProcessorLifecycle} for a {@link Node} and 
 * give it a {@link Supplier} to provide new instances of {@link Mp}s as the framework demands.
 * </p>
 */
public class MessageProcessor implements MessageProcessorLifecycle<Mp> {
    private final Supplier<? extends Mp> newMp;
    private final Set<String> messageTypes;
    private boolean isEvictable = false;
    private boolean hasOutput = false;

    public MessageProcessor(final Supplier<? extends Mp> newMp, final String... messageTypes) {
        if (newMp == null)
            throw new IllegalArgumentException("You must provide a Supplier that creates new " + Mp.class.getSimpleName() + "s.");
        this.newMp = newMp;
        this.messageTypes = new HashSet<>(Arrays.asList(messageTypes));
    }

    /**
     * The default setting for isEvictable is <code>false</code>. If your {@link Mp} implementation actually
     * implements {@link Mp#shouldBeEvicted()} then you should set this to <code>true</code> or Dempsy
     * will never call {@link Mp#shouldBeEvicted()}.
     */
    public MessageProcessor setEvictable(final boolean isEvictable) {
        this.isEvictable = isEvictable;
        return this;
    }

    /**
     * The default setting for hasOutput is <code>false</code>. If your {@link Mp} implementation actually
     * implements {@link Mp#ouput()} method then you should set this to <code>true</code> or Dempsy
     * will never call {@link Mp#ouput()}.
     */
    public MessageProcessor setOutput(final boolean hasOutputCapability) {
        this.hasOutput = hasOutputCapability;
        return this;
    }

    /**
     * This lifecycle phase is implemented by invoking the {@link Supplier} the was provided in the constructor.
     * 
     * @see MessageProcessorLifecycle#newInstance()
     */
    @Override
    public Mp newInstance() throws DempsyException {
        try {
            return newMp.get();
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    /**
     * This lifecycle phase is implemented by invoking the {@link Mp#activate(Object)} method and supplying
     * the key that this {@link Mp} instance will be responsible for.
     * 
     * @see MessageProcessorLifecycle#activate(Object, Object)
     */
    @Override
    public void activate(final Mp instance, final Object key) throws DempsyException {
        try {
            instance.activate(key);
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    /**
     * This lifecycle phase is implemented by invoking the {@link Mp#passivate()} method.
     * 
     * @see MessageProcessorLifecycle#passivate(Object)
     */
    @Override
    public void passivate(final Mp instance) throws DempsyException {
        try {
            instance.passivate();
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    /**
     * This main lifecycle phase is implemented by invoking the {@link Mp#handle(KeyedMessage)} method and supplying
     * the {@link KeyedMessage}
     * 
     * @see MessageProcessorLifecycle#invoke(Object, KeyedMessage)
     */
    @Override
    public List<KeyedMessageWithType> invoke(final Mp instance, final KeyedMessage message) throws DempsyException {
        try {
            return Arrays.asList(instance.handle(message));
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    /**
     * This lifecycle phase is implemented by invoking the {@link Mp#output()} method on the instance
     * 
     * @see MessageProcessorLifecycle#invokeOutput(Object)
     */
    @Override
    public List<KeyedMessageWithType> invokeOutput(final Mp instance) throws DempsyException {
        try {
            return Arrays.asList(instance.output());
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    /**
     * This lifecycle phase is implemented by invoking the {@link Mp#shouldBeEvicted()} method on the instance
     * 
     * @see MessageProcessorLifecycle#invokeEvictable(Object)
     */
    @Override
    public boolean invokeEvictable(final Mp instance) throws DempsyException {
        try {
            return instance.shouldBeEvicted();
        } catch (final RuntimeException rte) {
            throw new DempsyException(rte, true);
        }
    }

    @Override
    public boolean isEvictionSupported() {
        return isEvictable;
    }

    @Override
    public boolean isOutputSupported() {
        return hasOutput;
    }

    @Override
    public Set<String> messagesTypesHandled() {
        return messageTypes;
    }

    @Override
    public void validate() throws IllegalStateException {}

    @Override
    public void start(final ClusterId myCluster) {}

}
