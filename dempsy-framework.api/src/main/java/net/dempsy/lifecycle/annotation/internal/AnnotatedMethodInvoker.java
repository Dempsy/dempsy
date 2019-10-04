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

package net.dempsy.lifecycle.annotation.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will identify and invoke annotated methods, maintaining a thread-safe cache of those methods.
 * It currently supports the following three scenarios (which have disjoint constructors):
 * <ul>
 * <li>A single annotated getter method. <br>
 * The {@link #invokeGetter} method will look for an annotated no-parameter method on an arbitrary object,
 * and invoke that method. Behavior is undefined if there are multiple methods with the same annotation.
 * <li>Multiple annotated setter methods. <br>
 * The {@link #invokeSetter} method will look for annotated methods on a single class that take one parameter
 * of a "compatible" type to its argument. If there is no method that takes the exact type, it will
 * walk the class hierarchy of its argument to find a matching type.
 * <li>Multiple annotated single argument methods. <br>
 * The {@link #invokeMethod} method will look for annotated methods on a single class that take a <em>single</em>
 * parameter of a "compatible" type as its argument. If there is no method that takes the exact
 * type, it will walk the class hierarchy of its argument to find a matching type.
 * </ul>
 * Separate instances must be constructed for these each scenario, as they're based on different data.
 */
public class AnnotatedMethodInvoker {
    private final Class<? extends Annotation> annotationType;
    private final Map<Class<?>, Method> methods = new ConcurrentHashMap<Class<?>, Method>();

    /**
     * Constructs an instance to be used with annotated setter methods.
     *
     * @param objectKlass
     *     The class to be introspected for annotated methods.
     * @param annotationType
     *     Annotation that identifies setter or generic one argument methods.
     *
     * @throws IllegalArgumentException
     *     if the class does not have any single-argument methods with the specified annotation
     */
    public AnnotatedMethodInvoker(final Class<?> objectKlass, final Class<? extends Annotation> annotationType)
        throws IllegalArgumentException {
        this.annotationType = annotationType; // not relevant, but maybe useful for debugging

        for(final Method method: introspectAnnotationMultiple(objectKlass, annotationType, true)) {
            final Class<?>[] argTypes = method.getParameterTypes();
            if(argTypes.length == 1)
                methods.put(argTypes[0], method);
            else
                throw new IllegalArgumentException(
                    "The class " + objectKlass.getName() + " has the method " + method.getName() + " and is annotated with "
                        + annotationType.getSimpleName() + " but takes " + argTypes.length + " parameters when it must take exactly 1");
        }

        if(methods.size() == 0)
            throw new IllegalArgumentException(
                "class " + objectKlass.getName() + " does not have any 1-argument methods annotated with " +
                    annotationType.getSimpleName());
    }

    /**
     * Invokes the annotated getter method on the passed object (which may not be <code>null</code>), returning its result.
     *
     * @throws IllegalArgumentException
     *     if passed an object does not have any no-argument methods with the specified annotation
     * @throws IllegalAccessException
     *     if unable to invoke the annotated method
     * @throws InvocationTargetException
     *     if the invoked method threw an exception
     */
    public Object invokeGetter(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        final Class<?> klass = instance.getClass();
        Method method = methods.get(klass);
        if(method == null) {
            method = introspectAnnotationSingle(klass, annotationType);
            if((method == null) || (method.getParameterTypes().length != 0)) {
                throw new IllegalArgumentException(
                    "class " + klass.getName()
                        + " does not have any no-argument method annotated as @"
                        + annotationType.getName());
            }
            methods.put(klass, method);
        }

        return method.invoke(instance);
    }

    /**
     * Invokes the annotated setter appropriate to the passed value (must not be <code>null</code>).
     *
     * @throws IllegalArgumentException
     *     if there is no annotated method appropriate to the value
     * @throws IllegalAccessException
     *     if unable to invoke the annotated method
     * @throws InvocationTargetException
     *     if the invoked method threw an exception
     */
    public Object invokeSetter(final Object instance, final Object value)
        throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        final Class<?> valueClass = value.getClass();
        final Method method = getInvokableMethodForClass(valueClass);
        if(method == null) {
            throw new IllegalArgumentException(
                "class " + instance.getClass().getName()
                    + " does not have an annotated setter for values of type " + valueClass.getName());
        }

        return method.invoke(instance, value);
    }

    /**
     * Invokes the annotated single argument method appropriate to the passed value (must not be <code>null</code>) and returns its result.
     *
     * @throws IllegalArgumentException
     *     if there is no annotated method appropriate to the value
     * @throws IllegalAccessException
     *     if unable to invoke the annotated method
     * @throws InvocationTargetException
     *     if the invoked method threw an exception
     */
    public Object invokeMethod(final Object instance, final Object value)
        throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        final Class<?> valueClass = value.getClass();
        final Method method = getInvokableMethodForClass(valueClass);
        if(method == null) {
            throw new IllegalArgumentException(
                "class " + instance.getClass().getName()
                    + " does not have an annotated setter for values of type " + valueClass.getName());
        }

        return method.invoke(instance, value);
    }

    /**
     * Identifies whether there is an annotated setter appropriate to the passed value. This may be used as a pre-test for {@link #invokeSetter}, to avoid
     * catching <code>IllegalArgumentException</code>.
     */
    public boolean isValueSupported(final Object value) {
        return getInvokableMethodForClass(value.getClass()) != null;
    }

    /**
     * Examines the passed class and extracts a single method that is annotated with the specified annotation type, <code>null</code> if not methods are so
     * annotated. Behavior is undefined if multiple methods
     * have the specified annotation.
     */
    public static <T extends Annotation> Method introspectAnnotationSingle(final Class<?> klass, final Class<T> annotationType) {
        final List<Method> methods = introspectAnnotationMultiple(klass, annotationType, true);
        return (methods.size() > 0) ? methods.get(0) : null;
    }

    /**
     * Examines the passed class and extracts all methods that are annotated with the specified annotation type (may be none).
     */
    public static <T extends Annotation> List<Method> introspectAnnotationMultiple(final Class<?> klass, final Class<T> annotationType, final boolean recurse) {
        final List<Method> result = new ArrayList<Method>();
        for(final Method method: klass.getDeclaredMethods()) {
            if(method.getAnnotation(annotationType) != null)
                result.add(method);
        }

        if(!recurse)
            return result;

        final Class<?> superClazz = klass.getSuperclass();
        if(superClazz != null)
            result.addAll(introspectAnnotationMultiple(superClazz, annotationType, recurse));

        // Now do the interfaces.
        final Class<?>[] ifaces = klass.getInterfaces();
        if(ifaces != null && ifaces.length > 0)
            Arrays.stream(ifaces).forEach(iface -> result.addAll(introspectAnnotationMultiple(iface, annotationType, recurse)));
        return result;
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    public Method getInvokableMethodForClass(final Class<?> valueClass) {
        if(valueClass == null)
            return null;

        Method method = methods.get(valueClass);
        if(method != null)
            return method;

        // get the list of all classes and interfaces.
        // first classes.
        Class<?> clazz = valueClass.getSuperclass();
        while(clazz != null) {
            method = methods.get(clazz);
            if(method != null) {
                methods.put(valueClass, method);
                return method;
            }
            clazz = clazz.getSuperclass();
        }

        // now look through the interfaces.
        for(final Class<?> iface: valueClass.getInterfaces()) {
            method = methods.get(iface);
            if(method != null) {
                methods.put(valueClass, method);
                return method;
            }
        }

        return null;
    }

    public Set<Class<?>> getClassesHandled() {
        return this.methods.keySet();
    }

    public static class AnnotatedClass<A extends Annotation> {
        public final Class<?> clazz;
        public final A annotation;

        public AnnotatedClass(final Class<?> clazz, final A annotation) {
            this.clazz = clazz;
            this.annotation = annotation;
        }
    }

    /**
     * Get all annotation on the given class, plus all annotations on the parent classes
     *
     * @param clazz
     * @param annotation
     * @return
     */
    public static <A extends Annotation> List<AnnotatedClass<A>> allClassAnnotations(final Class<?> clazz, final Class<A> annotation, final boolean recurse) {
        final List<AnnotatedClass<A>> ret = new ArrayList<>();
        final A curClassAnnotation = clazz.getAnnotation(annotation);
        if(curClassAnnotation != null)
            ret.add(new AnnotatedClass<A>(clazz, curClassAnnotation));

        if(!recurse)
            return ret;

        final Class<?> superClazz = clazz.getSuperclass();
        if(superClazz != null)
            ret.addAll(allClassAnnotations(superClazz, annotation, recurse));

        // Now do the interfaces.
        final Class<?>[] ifaces = clazz.getInterfaces();
        if(ifaces != null && ifaces.length > 0)
            Arrays.stream(ifaces).forEach(iface -> ret.addAll(allClassAnnotations(iface, annotation, recurse)));
        return ret;
    }

    public int getNumMethods() {
        return methods.size();
    }

}
