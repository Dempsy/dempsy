package net.dempsy.lifecycle.simple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;

public class MessageProcessor implements MessageProcessorLifecycle<Mp> {
    private final Supplier<Mp> newMp;
    private final Set<String> messageTypes;
    private boolean isEvictable = false;
    private boolean hasOutput = false;

    public MessageProcessor(final Supplier<Mp> newMp, final String... messageTypes) {
        if (newMp == null)
            throw new IllegalArgumentException("You must provide a Supplier that creates new " + Mp.class.getSimpleName() + "s.");
        this.newMp = newMp;
        this.messageTypes = new HashSet<>(Arrays.asList(messageTypes));
    }

    public MessageProcessor setEvictable(final boolean isEvictable) {
        this.isEvictable = isEvictable;
        return this;
    }

    public MessageProcessor setOutput(final boolean hasOutputCapability) {
        this.hasOutput = hasOutputCapability;
        return this;
    }

    @Override
    public Mp newInstance() {
        return newMp.get();
    }

    @Override
    public void activate(final Mp instance, final Object key) throws IllegalArgumentException {
        instance.activate(key);
    }

    @Override
    public void passivate(final Mp instance) throws IllegalArgumentException {
        instance.passivate();
    }

    @Override
    public List<KeyedMessageWithType> invoke(final Mp instance, final KeyedMessage message) throws IllegalArgumentException {
        return Arrays.asList(instance.handle(message));
    }

    @Override
    public List<KeyedMessageWithType> invokeOutput(final Mp instance) throws IllegalArgumentException {
        return Arrays.asList(instance.output());
    }

    @Override
    public boolean invokeEvictable(final Mp instance) throws IllegalArgumentException {
        return instance.shouldBeEvicted();
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
