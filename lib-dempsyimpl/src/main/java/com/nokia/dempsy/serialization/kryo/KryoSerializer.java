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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

public class KryoSerializer<T> implements Serializer<T> 
{
   private static Logger logger = LoggerFactory.getLogger(KryoSerializer.class);
   private static class KryoHolder
   {
      public Kryo kryo = new Kryo();
      public Output output = new Output(256, 1024*1024*1024);
   }
   // need an object pool of Kryo instances since Kryo is not thread safe
   private ConcurrentLinkedQueue<KryoHolder> kryopool = new ConcurrentLinkedQueue<KryoHolder>();
   private List<Registration> registrations = null;
   
   public KryoSerializer() {}
   
   public KryoSerializer(List<Registration> reg)
   {
      registrations = Collections.unmodifiableList(reg);
   }
   
   public KryoSerializer(Registration... regs)
   {
      registrations = Arrays.asList(regs);
   }


   @Override
   public byte[] serialize(T object) throws SerializationException 
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         k.output.clear();
         k.kryo.writeClassAndObject(k.output, object);
         return k.output.toBytes();
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to serialize.",ke);
      }
      finally
      {
         if (k != null)
            kryopool.offer(k);
      }
   }
   
   @SuppressWarnings("unchecked")
   @Override
   public T deserialize(byte[] data) throws SerializationException 
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         return (T)(k.kryo.readClassAndObject(new Input(data)));  
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to deserialize.",ke);
      }
      finally
      {
         if (k != null)
            kryopool.offer(k);
      }
   }
   
   private KryoHolder getKryoHolder()
   {
      KryoHolder ret = kryopool.poll();
      if (ret == null)
      {
         ret = new KryoHolder();
         if (registrations != null)
         {
            for (Registration reg : registrations)
            {
               try
               {
                  ret.kryo.register(Class.forName(reg.classname), reg.id);
               }
               catch (ClassNotFoundException cnfe)
               {
                  logger.error("Cannot register the class " + SafeString.valueOf(reg.classname) + " with Kryo because the class couldn't be found.");
               }
            }
         }
      }
      return ret;
   }

}
