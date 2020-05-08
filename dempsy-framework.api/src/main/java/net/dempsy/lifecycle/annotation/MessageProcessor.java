/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.lifecycle.annotation;

import static net.dempsy.lifecycle.annotation.internal.MessageUtils.getAllMessageTypeTypeAnnotationValues;
import static net.dempsy.lifecycle.annotation.internal.MessageUtils.getMatchingMessageTypeTypeAnnotationValues;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import net.dempsy.DempsyException;
import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.lifecycle.annotation.internal.AnnotatedMethodInvoker;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.util.SafeString;

/**
 * This class holds the MP prototype, and supports invocation of MP methods on an instance.
 */
public class MessageProcessor<T> implements MessageProcessorLifecycle<T> {
    static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);

    private final T prototype;
    private final Class<?> mpClass;
    private final String mpClassName;

    private final String toStringValue;

    private final Method cloneMethod;
    private UpToThreeParameterMethod activationMethod;
    private final ZeroParameterMethod<byte[]> passivationMethod;
    private final List<Method> outputMethods;
    private final ZeroParameterMethod<Boolean> evictableMethod;
    private final AnnotatedMethodInvoker invocationMethods;
    private final Set<Class<?>> stopTryingToSendTheseTypes = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());
    private final Set<String> typesHandled;

    private final KeyExtractor keyExtractor = new KeyExtractor();
    private final boolean hasBulk;
    public final Method bulkMethod;

    public MessageProcessor(final T prototype) throws IllegalArgumentException, IllegalStateException {
        this.prototype = prototype;
        this.mpClass = prototype.getClass();
        this.mpClassName = mpClass.getName();
        this.toStringValue = getClass().getName() + "[" + mpClassName + "]";

        validateAsMP();
        cloneMethod = introspectClone();

        invocationMethods = new AnnotatedMethodInvoker(mpClass);
        hasBulk = invocationMethods.bulkMethod != null;
        bulkMethod = invocationMethods.bulkMethod;

        // =====================================================================
        // Find an activation method
        // =====================================================================
        final Set<Class<?>> handledTypes = invocationMethods.getClassesHandled();
        for(final Class<?> handledType: handledTypes) {
            final List<Method> messageKeyExtractors = AnnotatedMethodInvoker.introspectAnnotationMultiple(handledType, MessageKey.class, true);
            final Method messageKey = messageKeyExtractors != null && messageKeyExtractors.size() > 0 ? messageKeyExtractors.get(0) : null;
            if(messageKey == null)
                throw new IllegalStateException("The type handled by " + mpClassName + ", " + handledType.getName()
                    + " doesn't appear to have a message key denoted by the annotation @" + MessageKey.class.getSimpleName());
            final Class<?> messageKeyReturnType = messageKey.getReturnType();
            if(messageKeyReturnType == null || messageKeyReturnType == void.class)
                throw new IllegalStateException("The method to retrieve the key for the messages of type " + handledType.getName()
                    + " denoted by the method annotated with @" + MessageKey.class.getSimpleName() + " seems to have no return type (it's declared 'void').");

            if(activationMethod != null) {
                // TODO: verify that the type is compatible
            } else
                activationMethod = findActivationMethodCall(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Activation.class), messageKeyReturnType);
        }
        if(activationMethod == null)
            activationMethod = (i, k, m, s) -> {};
        // =====================================================================

        passivationMethod = findZeroParameterMethod(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Passivation.class), byte[].class);
        outputMethods = AnnotatedMethodInvoker.introspectAnnotationMultiple(mpClass, Output.class, true);
        evictableMethod = findZeroParameterMethod(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Evictable.class), Boolean.class, boolean.class,
            false);
        typesHandled = new HashSet<>(Arrays.asList(getMessageTypesFromMpClass(prototype.getClass())));
        if(invocationMethods.getNumMethods() > 0 && typesHandled == null || typesHandled.size() == 0)
            throw new IllegalArgumentException(
                "Cannot have a prototype Mp (" + SafeString.objectDescription(prototype) + ") that has no defined MessageTypes.");
    }

    /**
     * Creates a new instance from the prototype.
     */
    @SuppressWarnings("unchecked")
    @Override
    public T newInstance() throws DempsyException {
        return wrap(() -> (T)cloneMethod.invoke(prototype));
    }

    /**
     * Invokes the activation method of the passed instance.
     */
    @Override
    public void activate(final T instance, final Object key, final Object activatingMessage) throws DempsyException {
        wrap(() -> activationMethod.invoke(instance, key, activatingMessage, null));
    }

    /**
     * Invokes the passivation method of the passed instance. Will return the object's passivation data,
     * <code>null</code> if there is none.
     */
    @Override
    public void passivate(final T instance) throws DempsyException {
        wrap(() -> passivationMethod.invoke(instance));
    }

    /**
     * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing
     * <code>null</code> messages.
     */
    @Override
    public List<KeyedMessageWithType> invoke(final T instance, final KeyedMessage message) throws DempsyException {
        if(!isMessageSupported(message.message))
            throw new IllegalArgumentException(mpClassName + ": no handler for messages of type: " + message.message.getClass().getName());

        final Object returnValue = wrap(() -> invocationMethods.invokeMethod(instance, message.message));
        return returnValue == null ? null : convertToKeyedMessage(returnValue);
    }

    @Override
    public List<KeyedMessageWithType> invokeBulk(final T instance, final List<KeyedMessage> messages) {
        if(hasBulk) {
            final Object returnValue = wrap(() -> bulkMethod.invoke(instance, messages.stream()
                .map(m -> m.message)
                .collect(Collectors.toList())));
            return returnValue == null ? null : convertToKeyedMessage(returnValue);
        } else {
            final List<KeyedMessageWithType> ret = messages.stream()
                .map(m -> invoke(instance, m))
                .filter(m -> m != null)
                .filter(m -> m.size() > 0)
                .flatMap(ms -> ms.stream())
                .collect(Collectors.toList());
            return ret.size() > 0 ? ret : null;
        }
    }

    @Override
    public boolean isBulkDeliverySupported() {
        return hasBulk;
    }

    private static final List<KeyedMessageWithType> emptyKeyedMessageList = Collections.emptyList();

    /**
     * Invokes the output method, if it exists. If the instance does not have an
     * annotated output method, this is a no-op (this is simpler than requiring the
     * caller to check every instance).
     */
    @Override
    public List<KeyedMessageWithType> invokeOutput(final T instance) throws DempsyException {
        if(outputMethods == null)
            return emptyKeyedMessageList;

        final List<KeyedMessageWithType> ret = new ArrayList<>();
        for(final Method om: outputMethods) {
            final Object or = wrap(() -> om.invoke(instance));
            if(or != null) {
                ret.addAll(convertToKeyedMessage(or));
            }
        }

        return ret.size() == 0 ? null : ret;
    }

    @Override
    public boolean isOutputSupported() {
        return outputMethods != null && outputMethods.size() > 0;
    }

    @Override
    public boolean isEvictionSupported() {
        return evictableMethod != null;
    }

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     */
    @Override
    public boolean invokeEvictable(final T instance) throws DempsyException {
        return isEvictionSupported() ? (Boolean)wrap(() -> evictableMethod.invoke(instance)) : false;
    }

    /**
     * Determines whether this MP has a handler for the passed message. Will walk the message's class hierarchy if there
     * is not an exact match.
     */
    public boolean isMessageSupported(final Object message) {
        return invocationMethods.isValueSupported(message);
    }

    @Override
    public void validate() throws IllegalStateException {
        if(prototype != null) {
            if(!prototype.getClass().isAnnotationPresent(Mp.class))
                throw new IllegalStateException("Attempting to set an instance of \"" +
                    SafeString.valueOfClass(prototype) + "\" within the " +
                    Cluster.class.getSimpleName() +
                    " but it isn't identified as a MessageProcessor. Please annotate the class.");

            // the MessageHandler annotated methods are checked in the constructor
            checkOrInvokeValidStartMethod(false, null);

            final Method[] evictableMethods = prototype.getClass().getMethods();

            boolean foundEvictableMethod = false;
            Method evictableMethod = null;
            for(final Method method: evictableMethods) {
                if(method.isAnnotationPresent(Evictable.class)) {
                    if(foundEvictableMethod) {
                        throw new IllegalStateException("More than one method on the message processor of type \"" +
                            SafeString.valueOfClass(prototype)
                            + "\" is identified as a Evictable. Please annotate the appropriate method using @Evictable.");
                    }
                    foundEvictableMethod = true;
                    evictableMethod = method;
                }
            }

            if(evictableMethod != null) {
                if(evictableMethod.getReturnType() == null || !evictableMethod.getReturnType().isAssignableFrom(boolean.class))
                    throw new IllegalStateException(
                        "Evictable method \"" + SafeString.valueOf(evictableMethod) + "\" on the message processor of type \"" +
                            SafeString.valueOfClass(prototype)
                            + "\" should return boolean value. Please annotate the appropriate method using @Evictable.");
            }
        }
    }

    /**
     * To instances are equal if they wrap prototypes of the same class.
     */
    @Override
    public final boolean equals(final Object obj) {
        if(this == obj)
            return true;
        else if(obj instanceof MessageProcessor) {
            final MessageProcessor<?> that = (MessageProcessor<?>)obj;
            return this.prototype.getClass() == that.prototype.getClass();
        } else
            return false;
    }

    @Override
    public final int hashCode() {
        return prototype.getClass().hashCode();
    }

    @Override
    public String toString() {
        return toStringValue;
    }

    @Override
    public Set<String> messagesTypesHandled() {
        return typesHandled;
    }

    @Override
    public void start(final ClusterId clusterId) {
        checkOrInvokeValidStartMethod(true, clusterId);
    }

    @Override
    public ResourceManager manager() {
        return new ResourceManager();
    }

    public T getPrototype() {
        return prototype;
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    private void checkOrInvokeValidStartMethod(final boolean invoke, final ClusterId clusterId) throws IllegalStateException {
        Method startMethod = null;
        for(final Method method: prototype.getClass().getDeclaredMethods()) {
            if(method.isAnnotationPresent(Start.class)) {
                if(startMethod != null)
                    throw new IllegalStateException("Multiple methods on the message processor of type\""
                        + SafeString.valueOf(prototype)
                        + "\" is identified as a Start method. Please annotate at most one method using @Start.");
                startMethod = method;
            }
        }

        // if the start method takes a ClusterId or ClusterDefinition then pass it.
        if(startMethod != null) {
            final Class<?>[] parameterTypes = startMethod.getParameterTypes();
            boolean takesClusterId = false;
            if(parameterTypes != null && parameterTypes.length == 1) {
                if(ClusterId.class.isAssignableFrom(parameterTypes[0]))
                    takesClusterId = true;
                else {
                    throw new IllegalStateException("The method \"" + startMethod.getName() + "\" on " + SafeString.objectDescription(prototype) +
                        " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " +
                        "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter.");
                }
            } else if(parameterTypes != null && parameterTypes.length > 1) {
                throw new IllegalStateException("The method \"" + startMethod.getName() + "\" on " + SafeString.objectDescription(prototype) +
                    " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " +
                    "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter.");
            }
            if(invoke) {
                try {
                    if(takesClusterId)
                        startMethod.invoke(prototype, clusterId);
                    else
                        startMethod.invoke(prototype);
                } catch(final Exception e) {
                    LOGGER.error(MarkerFactory.getMarker("FATAL"), "can't run MP initializer " + startMethod.getName(), e);
                }
            }
        }
    }

    private void validateAsMP() throws IllegalStateException {
        if(mpClass.getAnnotation(Mp.class) == null)
            throw new IllegalStateException("MP class not annotated as MessageProcessor: " + mpClassName);
    }

    private Method introspectClone() throws IllegalStateException {
        try {
            // we do *NOT* allow inherited implementation
            return mpClass.getDeclaredMethod("clone");
        } catch(final SecurityException e) {
            throw new IllegalStateException("container does not have access to the message processor class \"" + mpClassName + "\"", e);
        } catch(final NoSuchMethodException e) {
            throw new IllegalStateException("The message processor class \"" + mpClassName + "\" does not declare the clone() method.");
        }
    }

    @FunctionalInterface
    protected static interface UpToThreeParameterMethod {
        void invoke(Object instance, Object o1, Object o2, byte[] state) throws IllegalAccessException, InvocationTargetException;
    }

    @FunctionalInterface
    protected static interface ZeroParameterMethod<RT> {
        RT invoke(Object instance) throws IllegalAccessException, InvocationTargetException;
    }

    protected <RT> ZeroParameterMethod<RT> findZeroParameterMethod(final Method method, final Class<RT> returnType) {
        return findZeroParameterMethod(method, returnType, null, true);
    }

    protected <RT> ZeroParameterMethod<RT> findZeroParameterMethod(final Method method, final Class<RT> returnType, final Class<?> alternateCastable,
        final boolean allowVoid) {
        try {
            if(method != null) {
                // there should be no parameters.
                if(method.getParameterCount() != 0)
                    throw new IllegalStateException(
                        "The method " + method.getName() + " on the class " + method.getDeclaringClass().getName() + " should take no parameters.");
                final Class<?> methodsReturnType = method.getReturnType();
                if(methodsReturnType == null || methodsReturnType == void.class) {
                    if(allowVoid)
                        return i -> {
                            method.invoke(i);
                            return null;
                        };

                    throw new IllegalStateException("The method " + method.getName() + " on the class " + method.getDeclaringClass().getName()
                        + " has a 'void' return type but it's expected to return something assignable to a " + returnType.getName());

                }
                if(!returnType.isAssignableFrom(methodsReturnType) && !(alternateCastable == null || alternateCastable.isAssignableFrom(methodsReturnType)))
                    throw new IllegalStateException(
                        "The method " + method.getName() + " on the class " + method.getDeclaringClass().getName() + " returns a " + methodsReturnType.getName()
                            + " but is expected to return something assignable to a " + returnType.getName());

                return i -> returnType.cast(method.invoke(i));
            }
            // the method is null, so we stub it out
            return i -> null;
        } catch(final IllegalStateException ise) {
            throw new DempsyException(ise, true);
        }
    }

    protected UpToThreeParameterMethod findActivationMethodCall(final Method method, final Class<?> keyClass) {
        if(method == null)
            return (i, k, m, s) -> {};

        try {
            // quick sanity check.
            if(byte[].class.isAssignableFrom(keyClass))
                throw new IllegalStateException(
                    "A message's key class cannot by assignable to a byte[] and be used in an @" + Activation.class.getSimpleName() + " method");

            final Class<?>[] parameterTypes = method.getParameterTypes();
            final int totalArguments = parameterTypes.length;
            if(totalArguments == 0)
                return (i, k, m, s) -> method.invoke(i);

            if(totalArguments > 3)
                throw new IllegalStateException(
                    "The method " + method.getName() + " on " + method.getDeclaringClass().getSimpleName()
                        + " takes too many parameters. It should take 2 or less.");

            int keyPosition = -1;
            int byteArrayPos = -1;
            int objectPos = -1;
            for(int i = 0; i < parameterTypes.length; i++) {
                final Class<?> parameter = parameterTypes[i];
                // parameter needs to be exactly Object.class but since
                // Object.class is assignable from anything, we need to check
                // that first.
                if(parameter.isAssignableFrom(Object.class)) {
                    if(objectPos != -1)
                        throw new IllegalStateException("It appears there's more than one parameter that takes exactly an Object in the @"
                            + Activation.class.getSimpleName() + " method on " + method.getDeclaringClass().getName());
                    objectPos = i;
                } else if(parameter.isAssignableFrom(keyClass)) {
                    if(keyPosition != -1)
                        throw new IllegalStateException(
                            "It appears there's more than one parameter on the @" + Activation.class.getSimpleName() + " method on "
                                + method.getDeclaringClass().getName() + " that can take the key of type \"" + keyClass.getSimpleName() + "\"");
                    keyPosition = i;
                } else if(byte[].class.isAssignableFrom(parameter)) {
                    if(byteArrayPos != -1)
                        throw new IllegalStateException(
                            "It appears there's more than one parameter on the @" + Activation.class.getSimpleName() + " method on "
                                + method.getDeclaringClass().getName() + " that can take a byte array.");
                    byteArrayPos = i;
                } else {
                    throw new IllegalStateException("The method " + method.getName() + " on " + method.getDeclaringClass().getSimpleName()
                        + " takes a \"" + parameter.getName() + "\" which doesn't seem to be either the key (a \"" + keyClass.getName() +
                        "\"), or an Object (where the activating message will be passed) or a byte[] where a prior passivated state will be provided");
                }
            }

            final int keyPositionF = keyPosition;
            final int objPositionF = objectPos;
            final int byteArrayPosF = byteArrayPos;
            final Object[] params = new Object[parameterTypes.length];
            return (i, k, m, s) -> {
                if(keyPositionF >= 0)
                    params[keyPositionF] = k;
                if(objPositionF >= 0)
                    params[objPositionF] = m;
                if(byteArrayPosF >= 0)
                    params[byteArrayPosF] = s;
                method.invoke(i, params);
            };
        } catch(final IllegalStateException ise) {
            throw new DempsyException(ise, true);
        }
    }

    // /**
    // * Class to handle method calls for activation and passivation
    // */
    // protected class MethodHandleX {
    // private final Method method;
    // private int keyPosition = -1;
    // private int totalArguments = 0;
    //
    // public MethodHandle(final Method method) {
    // this(method, null);
    // }
    //
    // public MethodHandle(final Method method, final Class<?> keyClass) {
    // this.method = method;
    // if(this.method != null) {
    // final Class<?>[] parameterTypes = method.getParameterTypes();
    // this.totalArguments = parameterTypes.length;
    // for(int i = 0; i < parameterTypes.length; i++) {
    // final Class<?> parameter = parameterTypes[i];
    // if(keyClass != null && parameter.isAssignableFrom(keyClass)) {
    // this.keyPosition = i;
    // }
    // }
    // }
    // }
    //
    // public Object invoke(final Object instance, final Object key)
    // throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    // if(this.method != null) {
    // final Object[] parameters = new Object[this.totalArguments];
    // if(this.keyPosition > -1)
    // parameters[this.keyPosition] = key;
    // return this.method.invoke(instance, parameters);
    // }
    // return null;
    // }
    //
    // public Object invoke(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    // return this.invoke(instance, null);
    // }
    //
    // public Method getMethod() {
    // return this.method;
    // }
    //
    // public boolean canThrowCheckedException(final Throwable th) {
    // if(method == null || th == null)
    // return false;
    //
    // for(final Class<?> cur: method.getExceptionTypes())
    // if(cur.isInstance(th))
    // return true;
    // return false;
    // }
    // }

    private List<KeyedMessageWithType> convertToKeyedMessage(final Object toSend) {
        final Class<?> messageClass = toSend.getClass();
        try {
            if(!stopTryingToSendTheseTypes.contains(messageClass))
                return keyExtractor.extract(toSend);
            else
                return emptyKeyedMessageList;
        } catch(final IllegalArgumentException e1) {
            stopTryingToSendTheseTypes.add(messageClass.getClass());
            LOGGER.warn("unable to retrieve key or message type from message: \"" + String.valueOf(toSend) +
                (toSend != null ? "\" of type \"" + SafeString.valueOf(toSend.getClass()) : "") +
                "\" Please make sure its has a simple getter appropriately annotated: " +
                e1.getLocalizedMessage(), e1); // no stack trace.
        } catch(final IllegalAccessException e1) {
            stopTryingToSendTheseTypes.add(messageClass.getClass());
            LOGGER.warn("unable to retrieve key from message: " + String.valueOf(toSend) +
                (toSend != null ? "\" of type \"" + SafeString.valueOf(toSend.getClass()) : "") +
                "\" Please make sure all annotated getter access is public: " +
                e1.getLocalizedMessage()); // no stack trace.
        } catch(final InvocationTargetException e1) {
            LOGGER.warn("unable to retrieve key from message: " + String.valueOf(toSend) +
                (toSend != null ? "\" of type \"" + SafeString.valueOf(toSend.getClass()) : "") +
                "\" due to an exception thrown from the getter: " +
                e1.getLocalizedMessage(), e1.getCause());
        }
        return emptyKeyedMessageList;
    }

    // =============================================================================================

    private static String[] getMessageTypesFromMpClass(final Class<?> mpClass) {
        final String[] classAnn = getAllMessageTypeTypeAnnotationValues(mpClass, false);

        // Find all of the handle methods and introspect their parameters.
        final String[] methAnn = AnnotatedMethodInvoker.introspectAnnotationMultiple(mpClass, MessageHandler.class, true).stream()
            // that this method takes exactly 1 parameter was already verified
            .map(handlerMethod -> {
                final String keyDescrim = handlerMethod.getAnnotation(MessageHandler.class).value();
                final boolean hasKeyDiscrim = keyDescrim != null && keyDescrim.length() > 0;

                final Class<?> paramClass = handlerMethod.getParameterTypes()[0];
                final String[] supportedMessageTypes = getMatchingMessageTypeTypeAnnotationValues(paramClass, hasKeyDiscrim ? keyDescrim : null);
                if(supportedMessageTypes == null || supportedMessageTypes.length == 0)
                    // this means there's a method annotated with @MessageTypes but the parameter isn't a
                    // MessageType
                    throw new IllegalStateException(
                        "The message processor " + mpClass.getName() + " has a @MessageHandler that takes a \"" + paramClass.getName()
                            + "\" but that class doesn't represent a @MessageType.");

                // let's validate that there's a possibility that a message will be routed here.

                return supportedMessageTypes;
            })
            .map(Arrays::stream)
            .flatMap(v -> v)
            .toArray(String[]::new);

        return Arrays.stream(new String[][] {classAnn,methAnn}).map(Arrays::stream).flatMap(v -> v).toArray(String[]::new);
    }

    @FunctionalInterface
    private static interface ThrowingSupplier<T> {
        public T run() throws IllegalAccessException, InvocationTargetException;
    }

    @FunctionalInterface
    private static interface ThrowingRunnable {
        public void run() throws IllegalAccessException, InvocationTargetException;
    }

    private static void wrap(final ThrowingRunnable runnable) throws DempsyException {
        wrap((ThrowingSupplier<Object>)() -> {
            runnable.run();
            return null;
        });
    }

    private static <T> T wrap(final ThrowingSupplier<T> runnable) throws DempsyException {
        try {
            return runnable.run();
        } catch(final InvocationTargetException e) {
            throw new DempsyException(e.getCause(), true);
        } catch(final IllegalAccessException e) {
            throw new DempsyException(e, true);
        }
    }

}
