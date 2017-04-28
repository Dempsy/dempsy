package net.dempsy.lifecycle.annotation.utils;

import static net.dempsy.lifecycle.annotation.internal.MessageUtils.unwindMessages;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.internal.AnnotatedMethodInvoker;
import net.dempsy.lifecycle.annotation.internal.MessageTypeExtractorFromMessages;
import net.dempsy.messages.KeyedMessageWithType;

public class KeyExtractor {

    private static class MteAndGetter {
        public final Method getter;
        public final MessageTypeExtractorFromMessages mtExtractor;

        public MteAndGetter(final Method getter, final MessageTypeExtractorFromMessages mtExtractor) {
            this.getter = getter;
            this.mtExtractor = mtExtractor;
        }
    }

    private final Map<Class<?>, MteAndGetter> cache = new HashMap<Class<?>, MteAndGetter>();

    public List<KeyedMessageWithType> extract(final Object toSend) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final List<Object> messages = new ArrayList<Object>();
        unwindMessages(toSend, messages);

        final ArrayList<KeyedMessageWithType> ret = new ArrayList<>(messages.size());

        for (final Object msg : messages) {
            final Class<?> messageClass = msg.getClass();
            MteAndGetter extractor = cache.get(messageClass);
            if (extractor == null) {
                extractor = new MteAndGetter(AnnotatedMethodInvoker.introspectAnnotationSingle(messageClass, MessageKey.class),
                        new MessageTypeExtractorFromMessages(messageClass));
                cache.put(messageClass, extractor);
            }

            if (extractor.getter == null)
                throw new IllegalArgumentException(
                        "The message object " + toSend.getClass().getSimpleName() + " doesn't seem to have a method annotated with "
                                + MessageKey.class.getSimpleName() + " so there's no way to route this message");
            final Object msgKeyValue = extractor.getter.invoke(toSend);

            ret.add(new KeyedMessageWithType(msgKeyValue, toSend, extractor.mtExtractor.get(toSend)));
        }

        return ret;
    }

}
