package net.dempsy.lifecycle.annotation.internal;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.internal.AnnotatedMethodInvoker.AnnotatedClass;
import net.dempsy.util.SafeString;

public class MessageUtils {

    public static String[] getAllMessageTypeTypeAnnotationValues(final Class<?> messageClass, final boolean recurse, final String particularDiscrim) {
        // search for the MessageType annotation on the class
        final List<AnnotatedClass<MessageType>> annotatedClasses = AnnotatedMethodInvoker.allTypeAnnotations(messageClass, MessageType.class, recurse);

        // get all of the keys from this
        final List<Method> methodsToGetKeys = AnnotatedMethodInvoker.introspectAnnotationMultiple(messageClass, MessageKey.class);
        // if there are more than 1 key then make sure that they all have keyDiscriminators.
        if(methodsToGetKeys.size() > 1)
            if(methodsToGetKeys.stream().anyMatch(m -> m.getAnnotation(MessageKey.class).value() == null))
                throw new IllegalStateException("The message class " + SafeString.valueOfClass(messageClass)
                    + " has multiple MessageKeys but at least one doesn't define a discriminator. If you have multiple message keys on a message they all must define a discriminator by setting a string on the MessageKey annotation.");

        // get all discriminators
        List<String> discriminators = methodsToGetKeys.stream()
            .map(m -> m.getAnnotation(MessageKey.class))
            .map(mka -> mka.value())
            .filter(discrim -> discrim != null && discrim.length() > 0)
            .collect(Collectors.toList());

        // did we pass a discriminator? If so, make sure it's on the list
        if(particularDiscrim != null && !discriminators.stream().anyMatch(d -> d.equals(particularDiscrim)))
            throw new IllegalStateException("We require a discriminated message key on " + SafeString.valueOfClass(messageClass) + " with a value of \""
                + particularDiscrim + "\" but there doesn't seem to be one.");

        if(particularDiscrim != null) // if we got here then the particularDiscrim is on the list. we're going to pair down the list to only that.
            discriminators = Arrays.asList(particularDiscrim);

        // combine with any MessageKey's that have discriminators set.
        return annotatedClasses.stream()
            .map(ac -> {
                final String[] values = ac.annotation.value();
                final String[] potentialMessageTypes = (values == null || values.length == 0) ? new String[] {ac.clazz.getName()} : values;
                return potentialMessageTypes;
            })
            .map(Arrays::stream)
            .flatMap(v -> v)
            .toArray(String[]::new);
    }

    /**
     * Return values from invoke's may be Iterables in which case there are many messages to be sent out.
     */
    public static void unwindMessages(final Object message, final List<Object> messages) {
        if(message instanceof Iterable) {
            @SuppressWarnings("rawtypes")
            final Iterator it = ((Iterable)message).iterator();
            while(it.hasNext())
                unwindMessages(it.next(), messages);
        } else
            messages.add(message);
    }

}
