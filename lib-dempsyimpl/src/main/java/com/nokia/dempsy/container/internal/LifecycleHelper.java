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

package com.nokia.dempsy.container.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.Evictable;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Passivation;
import com.nokia.dempsy.container.ContainerException;
import com.nokia.dempsy.container.MpContainer;
import com.nokia.dempsy.monitoring.StatsCollector;

/**
 * This class holds the MP prototype, and supports invocation of MP methods on an instance.
 */
public class LifecycleHelper
{
   private Object prototype;
   private Class<?> mpClass;
   private String mpClassName;

   private String toStringValue;

   private Method cloneMethod;
   private MethodHandle activationMethod;
   private MethodHandle passivationMethod;
   private Method outputMethod;
   private MethodHandle evictableMethod;
   private AnnotatedMethodInvoker invocationMethods;

   public LifecycleHelper(Object prototype) throws ContainerException
   {
      this.prototype = prototype;
      this.mpClass = prototype.getClass();
      this.mpClassName = mpClass.getName();
      this.toStringValue = getClass().getName() + "[" + mpClassName + "]";

      validateAsMP();
      cloneMethod = introspectClone();

      try
      {
         invocationMethods = new AnnotatedMethodInvoker(mpClass, MessageHandler.class);
      }
      catch(IllegalArgumentException ex)
      {
         throw new ContainerException(ex.getMessage());
      }
      Set<Class<?>> keys = invocationMethods.getMethods().keySet();
      for(Class<?> key: keys)
      {
         Method messageKey = AnnotatedMethodInvoker.introspectAnnotationSingle(key, MessageKey.class);
         activationMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Activation.class), 
               (messageKey == null)?null:messageKey.getReturnType());
      }
      passivationMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Passivation.class));
      outputMethod = AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Output.class);
      evictableMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Evictable.class));
   }

   /**
    * Creates a new instance from the prototype.
    */
   public Object newInstance() throws InvocationTargetException, IllegalAccessException
   {
      return cloneMethod.invoke(prototype);
   }

   /**
    * Invokes the activation method of the passed instance.
    */
   public void activate(Object instance, Object key, byte[] activationData) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      activationMethod.invoke(instance, key, activationData);
   }

   /**
    * Invokes the passivation method of the passed instance. Will return the object's passivation data, <code>null</code> if there is none.
    * 
    * @throws InvocationTargetException
    * @throws IllegalAccessException
    * @throws IllegalArgumentException
    */
   public byte[] passivate(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      return (byte[])passivationMethod.invoke(instance);
   }

   /**
    * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing <code>null</code> messages.
    * 
    * @throws ContainerException
    * @throws InvocationTargetException
    * @throws IllegalAccessException
    * @throws IllegalArgumentException
    */
   public Object invoke(Object instance, Object message, StatsCollector statsCollector) throws ContainerException, IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      Class<?> messageClass = message.getClass();
      if(!isMessageSupported(message))
         throw new ContainerException(mpClassName + ": no handler for messages of type: " + messageClass.getName());

      StatsCollector.TimerContext tctx = null;
      try
      {
         tctx = statsCollector.handleMessageStarted();
         return invocationMethods.invokeMethod(instance, message);
      }
      finally
      {
         if (tctx != null) tctx.stop();
      }
   }

   public String invokeDescription(MpContainer.Operation op, Object message)
   {
      Method method = null;
      Class<?> messageClass = void.class;
      if(op == MpContainer.Operation.output)
         method = outputMethod;
      else
      {
         if(message == null)
            return toStringValue + ".?(null)";

         messageClass = message.getClass();
         if(!isMessageSupported(message))
            return toStringValue + ".?(" + messageClass.getName() + ")";

         method = invocationMethods.getMethodForClass(message.getClass());
      }

      return toStringValue + "." + (method == null ? "?" : method.getName()) + "(" + messageClass.getSimpleName() + ")";
   }

   public Object getPrototype()
   {
      return prototype;
   }

   /**
    * Invokes the output method, if it exists. If the instance does not have an annotated output method, this is a no-op (this is simpler than requiring the
    * caller to check every instance).
    * 
    * @throws InvocationTargetException
    * @throws IllegalAccessException
    * @throws IllegalArgumentException
    */
   public Object invokeOutput(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      return (outputMethod != null) ? outputMethod.invoke(instance) : null;
   }
   
   /**
    * Invokes the evictable method on the provided instance.
    * If the evictable is not implemented, returns false.
    * 
    * @param instance
    * @return
    * @throws IllegalArgumentException
    * @throws IllegalAccessException
    * @throws InvocationTargetException
    */
   public boolean invokeEvictable(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
   {
      return isEvictableSupported() ? (Boolean)evictableMethod.invoke(instance): false;
   }

   /**
    * Determines whether the passed class matches the prototype's class.
    */
   public boolean isMatchingClass(Class<?> klass)
   {
      return klass.equals(prototype.getClass());
   }

   /**
    * Determines whether this MP has a handler for the passed message. Will walk the message's class hierarchy if there is not an exact match.
    */
   public boolean isMessageSupported(Object message)
   {
      return invocationMethods.isValueSupported(message);
   }

   /**
    * Determines whether this MP provides an output method.
    */
   public boolean isOutputSupported()
   {
      return outputMethod != null;
   }
   
   /**
    * Determines whether this MP provides an evictable method.
    */
   public boolean isEvictableSupported()
   {
      return evictableMethod.getMethod() != null;
   }

   /**
    * To instances are equal if they wrap prototypes of the same class.
    */
   @Override
   public final boolean equals(Object obj)
   {
      if(this == obj)
         return true;
      else if(obj instanceof LifecycleHelper)
      {
         LifecycleHelper that = (LifecycleHelper)obj;
         return this.prototype.getClass() == that.prototype.getClass();
      }
      else
         return false;
   }

   @Override
   public final int hashCode()
   {
      return prototype.getClass().hashCode();
   }

   @Override
   public String toString()
   {
      return toStringValue;
   }

   //----------------------------------------------------------------------------
   //  Internals
   //----------------------------------------------------------------------------

   private void validateAsMP() throws ContainerException
   {
      if(mpClass.getAnnotation(MessageProcessor.class) == null)
         throw new ContainerException("MP class not annotated as MessageProcessor: " + mpClassName);
   }

   private Method introspectClone() throws ContainerException
   {
      try
      {
         // we do *NOT* allow inherited implementation
         return mpClass.getDeclaredMethod("clone");
      }
      catch(SecurityException e)
      {
         throw new ContainerException("container does not have access to the message processor class \"" + mpClassName + "\"", e);
      }
      catch(NoSuchMethodException e)
      {
         throw new ContainerException("The message processor class \"" + mpClassName + "\" does not declare the clone() method.");
      }
   }
   
   /**
    * Class to handle method calls for activation and passivation 
    *
    */
   protected class MethodHandle
   {
      private Method method;
      private int keyPosition=-1;
      private int binayPosition=-1;
      private int totalArguments=0;

      public MethodHandle(Method method)
      {
         this(method, null);
      }

      public MethodHandle(Method method, Class<?> keyClass)
      {
         this.method = method;
         if(this.method != null)
         {
            Class<?>[] parameterTypes = method.getParameterTypes();
            this.totalArguments = parameterTypes.length;
            for(int i = 0; i < parameterTypes.length; i++)
            {
               Class<?> parameter = parameterTypes[i];
               if(parameter.isArray() && parameter.getComponentType().isAssignableFrom(byte.class))
               {
                  this.binayPosition = i;
               }
               else if(keyClass != null && parameter.isAssignableFrom(keyClass))
               {
                  this.keyPosition = i;
               }
            }
         }
      }
      
      public Object invoke(Object instance, Object key, byte[] data) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
      {
         if(this.method != null)
         {
            Object[] parameters = new Object[this.totalArguments];
            if(this.keyPosition>-1) parameters[this.keyPosition]=key;
            if(this.binayPosition>-1)parameters[this.binayPosition]=data;
            return this.method.invoke(instance, parameters);
         }
         return null;
      }
      public Object invoke(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
      {
         return this.invoke(instance, null, null);
      }
      
      public Method getMethod()
      {
         return this.method;
      }
   }
}
