package net.dempsy.lifecycle.annotation.utils;

import static net.dempsy.lifecycle.annotation.internal.MessageUtils.unwindMessages;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.internal.MessageUtils;
import net.dempsy.lifecycle.annotation.internal.MessageUtils.MessageTypeDetails;
import net.dempsy.messages.KeyedMessageWithType;

public class KeyExtractor {

    private final Map<Class<?>, MessageTypeDetails[]> cache = new ConcurrentHashMap<Class<?>, MessageTypeDetails[]>();

    public List<KeyedMessageWithType> extract(final Object toSend)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final List<Object> messages = new ArrayList<Object>();
        unwindMessages(toSend, messages);

        final ArrayList<KeyedMessageWithType> ret = new ArrayList<>(messages.size());

        for(final Object msg: messages) {
            final Class<?> messageClass = msg.getClass();
            final MessageTypeDetails[] extractors = cache.computeIfAbsent(messageClass,
                mc -> MessageUtils.getAllMessageTypeDetailsUsingAnnotationValues(mc, true).toArray(MessageTypeDetails[]::new));

            final int numExtractors = extractors.length;
            for(int i = 0; i < numExtractors; i++) {
                final MessageTypeDetails mtd = extractors[i];
                final Function<Object, Object> extractor = mtd.keyExtractorForThisType;
                if(extractor == null)
                    throw new IllegalArgumentException(
                        "The message object " + msg.getClass().getSimpleName() + " doesn't seem to have a method annotated with "
                            + MessageKey.class.getSimpleName() + " so there's no way to route this message");
                final Object msgKeyValue = extractor.apply(msg);

                ret.add(new KeyedMessageWithType(msgKeyValue, msg, mtd.messageType));
            }
        }

        return ret;
    }

}
