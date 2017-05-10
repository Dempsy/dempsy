package net.dempsy.lifecycle.annotation.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;

/**
 * This class is used to extract MessageType information from messages.
 */
public class MessageTypeExtractorFromMessages {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    public final String[] classDefinedMessageTypes;
    public final Method[] methodsThatGetsMessageTypes;

    public MessageTypeExtractorFromMessages(final Class<?> messageClass) throws IllegalStateException {
        // search for the MessageType annotation on the class
        classDefinedMessageTypes = MessageUtils.getAllMessageTypeTypeAnnotationValues(messageClass, true);

        final List<Method> mtMethods = new ArrayList<>();
        for (final Method method : messageClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(MessageType.class)) {
                // make sure it qualifies.
                if (method.getParameterCount() > 0)
                    throw new IllegalStateException("The message class " + messageClass.getName() +
                            " has the method " + method.getName() + " annotated with  " + MessageType.class.getSimpleName() +
                            " but it takes parameters when it shouldn't.");

                if (!String[].class.isAssignableFrom(method.getReturnType()))
                    throw new IllegalStateException("The message class " + messageClass.getName() +
                            " has the method " + method.getName() + " annotated with  " + MessageType.class.getSimpleName() +
                            " but doesn't return an array of Strings.");

                final MessageType mt = method.getAnnotation(MessageType.class);
                final String[] values = mt.value();
                if (values != null && values.length > 0)
                    LOGGER.warn("Annotation " + MessageType.class.getSimpleName() + " on the method " + method.getName()
                            + " includes a list of values. These are ignored when this annotation is used on a method of a message.");
                mtMethods.add(method);
            }
        }

        methodsThatGetsMessageTypes = (mtMethods.size() == 0) ? null : mtMethods.toArray(new Method[mtMethods.size()]);
    }

    public String[] get(final Object message) {
        return doIt(message, false);
    }

    public String[] getThrows(final Object message) {
        return doIt(message, true);
    }

    private String[] doIt(final Object message, final boolean iThrow) {

        if (methodsThatGetsMessageTypes == null)
            return classDefinedMessageTypes;

        else {
            final String[] subret = Arrays.stream(methodsThatGetsMessageTypes).map(m -> {
                try {
                    return (String[]) m.invoke(message);
                } catch (IllegalAccessException | IllegalArgumentException e) {
                    LOGGER.warn("Can't invoke method " + m.getName() + " due to the following exception", e);
                    if (iThrow)
                        throw new DempsyException(e, true);
                    return new String[0];
                } catch (final InvocationTargetException e) {
                    LOGGER.warn("Can't invoke method " + m.getName() + " due to the following exception", e.getTargetException());
                    if (iThrow)
                        throw new DempsyException(e.getTargetException(), true);
                    return new String[0];
                }
            }).map(Arrays::stream).flatMap(v -> v).toArray(String[]::new);

            if (classDefinedMessageTypes != null)
                return Arrays.stream(new String[][] { classDefinedMessageTypes, subret }).map(Arrays::stream).flatMap(v -> v)
                        .toArray(String[]::new);
            else
                return subret;
        }
    }

}