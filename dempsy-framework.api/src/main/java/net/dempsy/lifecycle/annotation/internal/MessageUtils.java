package net.dempsy.lifecycle.annotation.internal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.internal.AnnotatedMethodInvoker.AnnotatedClass;

public class MessageUtils {

    public static String[] getAllMessageTypeTypeAnnotationValues(final Class<?> messageClass, final boolean recurse) {
        // search for the MessageType annotation on the class
        final List<AnnotatedClass<MessageType>> annotatedClasses = AnnotatedMethodInvoker.allTypeAnnotations(messageClass, MessageType.class,
                recurse);
        return annotatedClasses.stream().map(ac -> {
            final String[] values = ac.annotation.value();
            return (values == null || values.length == 0) ? new String[] { ac.clazz.getName() } : values;
        }).map(Arrays::stream).flatMap(v -> v).toArray(String[]::new);
    }

    /**
     * Return values from invoke's may be Iterables in which case there are many messages to be sent out.
     */
    public static void unwindMessages(final Object message, final List<Object> messages) {
        if (message instanceof Iterable) {
            @SuppressWarnings("rawtypes")
            final Iterator it = ((Iterable) message).iterator();
            while (it.hasNext())
                unwindMessages(it.next(), messages);
        } else
            messages.add(message);
    }

}
