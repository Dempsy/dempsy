/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.container.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 *  This class will identify and invoke annotated methods, maintaining a thread-safe
 *  cache of those methods. It currently supports the following three scenarios (which
 *  have disjoint constructors):
 *  <ul>
 *  <li> A single annotated getter method. <br>
 *       The {@link #invokeGetter} method will look for an annotated no-parameter
 *       method on an arbitrary object, and invoke that method. Behavior is undefined
 *       if there are multiple methods with the same annotation.
 *  <li> Multiple annotated setter methods. <br>
 *       The {@link #invokeSetter} method will look for annotated methods on a single
 *       class that take one parameter of a "compatible" type to its argument. If there
 *       is no method that takes the exact type, it will walk the class hierarchy of
 *       its argument to find a matching type.
 *  <li>Multiple annotated single argument methods. <br>
 *      The {@link #invokeMethod} method will look for annotated methods on a single
 *      class that take a <em>single</em> parameter of a "compatible" type as its argument.
 *      If there is no method that takes the exact type, it will walk the class hierarchy
 *      of its argument to find a matching type.
 *  </ul>
 *  Separate instances must be constructed for these each scenario, as they're based
 *  on different data.
 */
public class AnnotatedMethodInvoker
{
   private Class<? extends Annotation> annotationType;
   private Map<Class<?>,Method> methods = new ConcurrentHashMap<Class<?>, Method>();


   /**
    *  Constructs an instance to be used with annotated getter methods.
    *
    *  @param  annotationType Annotation that identifies getter methods.
    */
   public AnnotatedMethodInvoker(Class<? extends Annotation> annotationType)
   {
      this.annotationType = annotationType;
   }


   /**
    *  Constructs an instance to be used with annotated setter methods.
    *
    *  @param  objectKlass    The class to be introspected for annotated methods.
    *  @param  annotationType Annotation that identifies setter or generic one argument methods.
    *
    *  @throws IllegalArgumentException if the class does not have any single-argument
    *          methods with the specified annotation
    */
   public AnnotatedMethodInvoker(Class<?> objectKlass, Class<? extends Annotation> annotationType)
   throws IllegalArgumentException
   {
      this.annotationType = annotationType;  // not relevant, but maybe useful for debugging

      for (Method method : introspectAnnotationMultiple(objectKlass, annotationType))
      {
         Class<?>[] argTypes = method.getParameterTypes();
         if (argTypes.length == 1)
            methods.put(argTypes[0], method);
      }

      if (methods.size() == 0)
      {
         throw new IllegalArgumentException(
               "class " + objectKlass.getName()
               + " does not have any 1-argument methods annotated with "
               + annotationType.getName());
      }
   }


   /**
    *  Invokes the annotated getter method on the passed object (which may not be
    *  <code>null</code>), returning its result.
    *
    *  @throws IllegalArgumentException if passed an object does not have any
    *          no-argument methods with the specified annotation
    *  @throws IllegalAccessException if unable to invoke the annotated method
    *  @throws InvocationTargetException if the invoked method threw an exception
    */
   public Object invokeGetter(Object instance)
   throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      Class<?> klass = instance.getClass();
      Method method = methods.get(klass);
      if (method == null)
      {
         method = introspectAnnotationSingle(klass, annotationType);
         if ((method == null) || (method.getParameterTypes().length != 0))
         {
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
    *  Invokes the annotated setter appropriate to the passed value (must not be
    *  <code>null</code>).
    *
    *  @throws IllegalArgumentException if there is no annotated method appropriate
    *          to the value
    *  @throws IllegalAccessException if unable to invoke the annotated method
    *  @throws InvocationTargetException if the invoked method threw an exception
    */
   public Object invokeSetter(Object instance, Object value)
   throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      Class<?> valueClass = value.getClass();
      Method method = getMethodForClass(valueClass);
      if (method == null)
      {
         throw new IllegalArgumentException(
               "class " + instance.getClass().getName()
               + " does not have an annotated setter for values of type " + valueClass.getName());
      }

      return method.invoke(instance, value);
   }

   /**
    *  Invokes the annotated single argument method appropriate to the passed value (must not be
    *  <code>null</code>) and returns its result.
    *
    *  @throws IllegalArgumentException if there is no annotated method appropriate
    *          to the value
    *  @throws IllegalAccessException if unable to invoke the annotated method
    *  @throws InvocationTargetException if the invoked method threw an exception
    */
   public Object invokeMethod(Object instance, Object value)
   throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      Class<?> valueClass = value.getClass();
      Method method = getMethodForClass(valueClass);
      if (method == null)
      {
         throw new IllegalArgumentException(
               "class " + instance.getClass().getName()
               + " does not have an annotated setter for values of type " + valueClass.getName());
      }

      return method.invoke(instance, value);
   }
   
   /**
    *  Identifies whether there is an annotated setter appropriate to the passed
    *  value. This may be used as a pre-test for {@link #invokeSetter}, to avoid
    *  catching <code>IllegalArgumentException</code>.
    */
   public boolean isValueSupported(Object value)
   {
      return getMethodForClass(value.getClass()) != null;
   }


   /**
    *  Examines the passed class and extracts a single method that is annotated
    *  with the specified annotation type, <code>null</code> if not methods are
    *  so annotated. Behavior is undefined if multiple methods have the specifed
    *  annotation.
    */
   public static <T extends Annotation> Method introspectAnnotationSingle(
         Class<?> klass, Class<T> annotationType)
   {
      List<Method> methods = introspectAnnotationMultiple(klass, annotationType);
      return (methods.size() > 0)
             ? methods.get(0)
             : null;
   }


   /**
    *  Examines the passed class and extractsall methods that are annotated
    *  with the specified annotation type (may be none).
    */
   public static <T extends Annotation> List<Method> introspectAnnotationMultiple(
         Class<?> klass, Class<T> annotationType)
   {
      List<Method> result = new ArrayList<Method>();
      for (Method method : klass.getMethods())
      {
         if (method.getAnnotation(annotationType) != null)
            result.add(method);
      }
      return result;
   }


//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

   public Method getMethodForClass(Class<?> valueClass)
   {
      if (valueClass == null)
         return null;
      
      Method method = methods.get(valueClass);
      if (method != null)
         return method;

      // stop recursion once we hit Object
      valueClass = valueClass.getSuperclass();
      if (valueClass == null)
         return null;

      // once we learn the handler method, we'll remember it to avoid future recursion
      method = getMethodForClass(valueClass);
      if (method != null)
         methods.put(valueClass, method);

      return method;
   }
   
   public Map<Class<?>,Method> getMethods()
   {
      return this.methods;
   }
}
