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

package com.nokia.dempsy.serialization.kryo;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

/**
 * This is the implementation of the Kryo based serialization for Dempsy.
 * It can be configured with registered classes using Spring by passing
 * a list of {@link Registration} instances to the constructor.
 */
public class KryoSerializer<T> implements Serializer<T> 
{
   private static Logger logger = LoggerFactory.getLogger(KryoSerializer.class);
   private static class KryoHolder
   {
      private static final byte[] park = new byte[0];
      public Kryo kryo = new Kryo();
      public Output output = new Output();
      public Input input = new Input();
      
      public void clearOutput() { output.setBuffer(park,Integer.MAX_VALUE); }
      public void clearInput() { input.setBuffer(park); }
   }
   // need an object pool of Kryo instances since Kryo is not thread safe
   private ConcurrentLinkedQueue<KryoHolder> kryopool = new ConcurrentLinkedQueue<KryoHolder>();
   private List<Registration> registrations = null;
   private KryoOptimizer optimizer = null;
   private boolean requireRegistration = false;
   
   /**
    * Create an unconfigured default {@link KryoSerializer} with no registered classes.
    */
   public KryoSerializer() {}
   
   /**
    * Create an {@link KryoSerializer} with the provided registrations. This can be
    * used from a Spring configuration.
    */
   public KryoSerializer(Registration... regs)
   {
      registrations = Arrays.asList(regs); 
   }
   
   /**
    * Create an {@link KryoSerializer} with the provided registrations and Application specific
    * Optimizer. This can be used from a Spring configuration.
    */
   public KryoSerializer(KryoOptimizer optimizer, Registration... regs)
   {
      registrations = Arrays.asList(regs);
      this.optimizer = optimizer;
   }
   
   /**
    * Set the optimizer. This is provided for a dependency injection framework to use. If it's called
    * @param optimizer
    */
   public synchronized void setKryoOptimizer(KryoOptimizer optimizer)
   {
      this.optimizer= optimizer;
      kryopool.clear(); // need to create new Kryo's.
   }
   
   /**
    * You can require Kryo to serialize only registered classes by passing '<code>true</code>' to
    * setKryoRegistrationRequired. The default is '<code>false</code>'.
    */
   public synchronized void setKryoRegistrationRequired(boolean requireRegistration)
   {
      if (this.requireRegistration != requireRegistration)
      {
         this.requireRegistration = requireRegistration;
         kryopool.clear();
      }
   }

   @Override
   public void serialize(T object, MessageBufferOutput buffer) throws SerializationException 
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         final Output output = k.output;
         // this will allow kryo to grow the buffer as needed.
         output.setBuffer(buffer.getBuffer(),Integer.MAX_VALUE);
         output.setPosition(buffer.getPosition()); // set the position to where we already are.
         k.kryo.writeClassAndObject(output, object);
         // if we resized then we need to adjust the message buffer
         if (output.getBuffer() != buffer.getBuffer())
            buffer.replace(output.getBuffer());
         buffer.setPosition(output.position());
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to serialize.",ke);
      }
      catch (IllegalArgumentException e) // this happens when requiring registration but serializing an unregistered class
      {
         throw new SerializationException("Failed to serialize " + SafeString.objectDescription(object) + 
               " (did you require registration and attempt to serialize an unregistered class?)", e);
      }
      finally
      {
         if (k != null)
         {
            k.clearOutput();
            kryopool.offer(k);
         }
      }
   }
   
   @SuppressWarnings("unchecked")
   @Override
   public T deserialize(MessageBufferInput data) throws SerializationException 
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         final Input input = k.input;
         input.setBuffer(data.getBuffer(),data.getPosition(),data.getLimit());
         T ret = (T)(k.kryo.readClassAndObject(input));
         data.setPosition(input.position()); // forward to where Kryo finished.
         return ret;
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to deserialize.",ke);
      }
      catch (IllegalArgumentException e) // this happens when requiring registration but deserializing an unregistered class
      {
         throw new SerializationException("Failed to deserialize. Did you require registration and attempt to deserialize an unregistered class?", e);
      }
      finally
      {
         if (k != null)
         {
            k.clearInput();
            kryopool.offer(k);
         }
      }
   }
   
   private KryoHolder getKryoHolder()
   {
      KryoHolder ret = kryopool.poll();
      if (ret == null)
      {
         ret = new KryoHolder();
         if (requireRegistration)
            ret.kryo.setRegistrationRequired(requireRegistration);
         
         if (optimizer != null)
         {
            try { optimizer.preRegister(ret.kryo); }
            catch (Throwable th) { logger.error("Optimizer for KryoSerializer \"" + SafeString.valueOfClass(optimizer) + 
                  "\" threw and unepexcted exception.... continuing.",th); }
         }

         if (registrations != null)
         {
            for (Registration reg : registrations)
            {
               try
               {
                  if (reg.id == -1)
                     ret.kryo.register(Class.forName(reg.classname));
                  else
                     ret.kryo.register(Class.forName(reg.classname), reg.id);
               }
               catch (ClassNotFoundException cnfe)
               {
                  logger.error("Cannot register the class " + SafeString.valueOf(reg.classname) + " with Kryo because the class couldn't be found.");
               }
            }
         }
         
         if (optimizer != null)
         {
            try { optimizer.postRegister(ret.kryo); }
            catch (Throwable th) { logger.error("Optimizer for KryoSerializer \"" + SafeString.valueOfClass(optimizer) + 
                  "\" threw and unepexcted exception.... continuing.",th); }
         }
      }
      return ret;
   }

}
