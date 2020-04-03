package net.dempsy.lifecycle.annotation.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.dempsy.DempsyException;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.internal.AnnotatedMethodInvoker.AnnotatedClass;
import net.dempsy.util.SafeString;

public class MessageUtils {

    public static String[] getAllMessageTypeTypeAnnotationValues(final Class<?> messageClass, final boolean recurse) {
        return getAllMessageTypeTypeAnnotationValuesAsList(messageClass, recurse, true, null).stream().map(mt -> mt.messageType).toArray(String[]::new);
    }

    public static List<MessageTypeDetails> getAllMessageTypeDetailsUsingAnnotationValues(final Class<?> messageClass, final boolean recurse) {
        return getAllMessageTypeTypeAnnotationValuesAsList(messageClass, recurse, true, null);
    }

    public static String[] getMatchingMessageTypeTypeAnnotationValues(final Class<?> messageClass, final String particularDiscrim) {
        return getAllMessageTypeTypeAnnotationValuesAsList(messageClass, false, false, particularDiscrim).stream().map(mt -> mt.messageType)
            .toArray(String[]::new);
    }

    public static class MessageTypeDetails {
        public final String messageType;
        public final Function<Object, Object> keyExtractorForThisType;

        public MessageTypeDetails(final String baseMessageType, final MethodWithDiscrim mwd) {
            final String discrim = mwd.discrim;
            this.messageType = discrim == null || discrim.length() == 0 ? baseMessageType : (baseMessageType + "(" + discrim + ")");
            final Method keyGetter = mwd.keyGetter;
            this.keyExtractorForThisType = o -> {
                try {
                    return keyGetter.invoke(o);
                } catch(IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new DempsyException(
                        "Failed to extract key from \"" + SafeString.objectDescription(o) + "\" using the method \"" + keyGetter.getName() + "\"", e, true);
                }
            };
        }
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

    private static List<MessageTypeDetails> getAllMessageTypeTypeAnnotationValuesAsList(final Class<?> messageClass, final boolean recurse,
        final boolean getAllIgnoringParticularDiscrim, final String particularDiscrim) {
        final List<MessageTypeDetails> results = getAllMessageTypeTypeAnnotationValuesAtThisLevel(messageClass, getAllIgnoringParticularDiscrim,
            particularDiscrim);

        if(!recurse)
            return results;

        final Class<?> superClazz = messageClass.getSuperclass();
        if(superClazz != null)
            results.addAll(getAllMessageTypeTypeAnnotationValuesAsList(superClazz, recurse, getAllIgnoringParticularDiscrim, particularDiscrim));

        // Now do the interfaces.
        final Class<?>[] ifaces = messageClass.getInterfaces();
        if(ifaces != null && ifaces.length > 0)
            Arrays.stream(ifaces).forEach(
                iface -> results.addAll(getAllMessageTypeTypeAnnotationValuesAsList(iface, recurse, getAllIgnoringParticularDiscrim, particularDiscrim)));
        return results;

    }

    private static class MethodWithDiscrim {
        public String discrim;
        public MessageKey annotation;
        public Method keyGetter;

        public MethodWithDiscrim(final Method method) {
            keyGetter = method;
            this.annotation = method.getAnnotation(MessageKey.class);
            this.discrim = this.annotation.value();
        }
    }

    private static List<MessageTypeDetails> getAllMessageTypeTypeAnnotationValuesAtThisLevel(final Class<?> messageClass,
        final boolean getAllIgnoringParticularDiscrim, final String particularDiscrim) {

        // search for the MessageType annotation on the class
        final List<AnnotatedClass<MessageType>> annotatedClasses = AnnotatedMethodInvoker.allClassAnnotations(messageClass, MessageType.class, false);

        // get all of the keys from this
        final List<Method> methodsToGetKeys = AnnotatedMethodInvoker.introspectAnnotationMultiple(messageClass, MessageKey.class, false);

        // if there are no keys at this level of the class hierarchy but there are annotatedClasses then we have a problem.
        if(annotatedClasses.size() > 0)
            if(methodsToGetKeys.size() == 0)
                throw new IllegalStateException("The message class " + SafeString.valueOf(messageClass)
                    + " has the MessageType annotation but doesn't define any MessageKeys. MessageKeys are not inherited and must be defined at the class hierarchy level where the MessageType annoatation is used.");

        // if there are more than 1 key then make sure that they all have keyDiscriminators.
        if(methodsToGetKeys.size() > 1)
            if(methodsToGetKeys.stream().map(m -> m.getAnnotation(MessageKey.class).value()).anyMatch(v -> v == null || v.length() == 0))
                throw new IllegalStateException("The message class " + SafeString.valueOf(messageClass)
                    + " has multiple MessageKeys but at least one doesn't define a discriminator. If you have multiple message keys on a message they all must define a discriminator by setting a string on the MessageKey annotation.");

        // get all discriminators
        final List<MethodWithDiscrim> discriminators = new ArrayList<>(methodsToGetKeys.stream().map(MethodWithDiscrim::new).collect(Collectors.toList()));

        // did we pass a discriminator? If so, make sure it's on the list
        if(!getAllIgnoringParticularDiscrim) {
            // if we're looking for a discriminator
            if(particularDiscrim != null && particularDiscrim.length() > 0) {
                // find the ONE we care about
                final MethodWithDiscrim particular = discriminators.stream().filter(d -> particularDiscrim.equals(d.discrim)).findAny().orElse(null);

                // ... but the one we're looking for isn't part of the class
                if(particular == null)
                    throw new IllegalStateException("We require a discriminated message key on " + SafeString.valueOf(messageClass) + " with a value of \""
                        + particularDiscrim + "\" but there doesn't seem to be one.");

                // if we got here then the particularDiscrim is on the list. we're going to pair down the list to only that.
                discriminators.clear();
                discriminators.add(particular);
            } else {
                // we're looking for a particular discriminator, but that is an EMPTY discriminator
                if(discriminators.size() > 1) // 1 is the MethodWithDiscrim that has an empty discrim.
                    throw new IllegalStateException("There's an ambiguous key on " + SafeString.valueOf(messageClass)
                        + ". You will need to make sure you select the appropriate key on each Mp MessageHandler.");
            }
        }

        // combine with any MessageKey's that have discriminators set.
        return new ArrayList<>(annotatedClasses.stream()
            .map(ac -> {
                final String[] values = ac.annotation.value();
                final String[] potentialMessageTypes = (values == null || values.length == 0) ? new String[] {ac.clazz.getName()} : values;
                return potentialMessageTypes;
            })
            .map(Arrays::stream)
            .flatMap(v -> v)
            .map(mt -> discriminators.stream()
                .map(d -> new MessageTypeDetails(mt, d)))
            .flatMap(v -> v)
            .collect(Collectors.toList()));

    }

}
