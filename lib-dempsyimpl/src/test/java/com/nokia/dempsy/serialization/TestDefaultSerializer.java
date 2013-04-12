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

package com.nokia.dempsy.serialization;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.LongSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.serialization.java.JavaSerializer;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;
import com.nokia.dempsy.serialization.kryo.KryoSerializer;
import com.nokia.dempsy.serialization.kryo.Registration;

public class TestDefaultSerializer 
{
   private static final int TEST_NUMBER = 42;
   private static final String TEST_STRING = "life, the universe and everything";
   private static final long baseTimeoutMillis = 20000;
   private static final int numThreads = 5;
   
   private MockClass o1 = new MockClass(TEST_NUMBER, TEST_STRING);
   
   @Test
   public void testJavaSerializeDeserialize() throws Throwable
   {
      runSerializer(new JavaSerializer<MockClass>());
   }
   
   @Test
   public void testKryoSerializeDeserialize() throws Throwable
   {
      runSerializer(new KryoSerializer<MockClass>());
   }
   
   @Test
   public void testKryoSerializeDeserializeWithRegister() throws Throwable
   {
      KryoSerializer<MockClass> ser1 = new KryoSerializer<MockClass>();
      runSerializer(ser1);
      KryoSerializer<MockClass> ser2 = 
            new KryoSerializer<MockClass>(new Registration(MockClass.class.getName(),10));
      runSerializer(ser2);
      
      MessageBufferOutput mb = new MessageBufferOutput();
      
      ser1.serialize(o1,mb);
      byte[] d1 = Arrays.copyOf(mb.getBuffer(),mb.getPosition());
      mb.reset();
      ser2.serialize(o1,mb);
      byte[] d2 = Arrays.copyOf(mb.getBuffer(),mb.getPosition());
      assertTrue(d2.length < d1.length);
   }
   
   @Test(expected=SerializationException.class)
   public void testKryoSerializeWithRegisterFail() throws Throwable
   {
      KryoSerializer<MockClass> ser1 = new KryoSerializer<MockClass>();
      ser1.setKryoRegistrationRequired(true);
      runSerializer(ser1);
   }

   @Test(expected=SerializationException.class)
   public void testKryoDeserializeWithRegisterFail() throws Throwable
   {
      KryoSerializer<MockClass> ser1 = new KryoSerializer<MockClass>();
      KryoSerializer<MockClass> ser2 = new KryoSerializer<MockClass>();
      ser2.setKryoRegistrationRequired(true);
      MessageBufferOutput mb = new MessageBufferOutput();
      ser1.serialize(new MockClass(),mb);
      
      MessageBufferInput data = new MessageBufferInput(mb.getBuffer());
      ser2.deserialize(data);
   }

   private void runSerializer(Serializer<MockClass> serializer) throws Throwable
   {
      MessageBufferOutput mb = new MessageBufferOutput();
      serializer.serialize(o1,mb);
      byte[] data = mb.getBuffer();
      assertNotNull(data);
      MockClass o2 = serializer.deserialize(new MessageBufferInput(data));
      assertNotNull(o2);
      assertEquals(o1, o2);
   }
   
   @SuppressWarnings("serial")
   public static class Mock2 implements Serializable
   {
      private final int i;
      private final MockClass mock;
      
      public Mock2() 
      {
         i = 5;
         mock = null;
      }
      
      public Mock2(int i, MockClass mock)
      { 
         this.i = i;
         this.mock = mock;
      }
      
      public int getInt() { return i; }
      
      public MockClass getMockClass() { return mock; }
      
      public boolean equals(Object obj)
      {
         Mock2 o = (Mock2)obj;
         return o.i == i && mock.equals(o.mock);
      }
   }
   
   @SuppressWarnings("serial")
   public static class Mock3 extends Mock2
   {
      public int myI = -1;
      private UUID uuid = UUID.randomUUID();
      
      public Mock3() {}
      
      public Mock3(int i, MockClass mock)
      {
         super(i,mock);
         this.myI = i;
      }
      
      public boolean equals(Object obj)
      {
         Mock3 o = (Mock3)obj;
         return super.equals(obj) && myI == o.myI && uuid.equals(o.uuid);
      }
      
      public UUID getUUID() { return uuid; }
   }
   
   @Test
   public void testKryoWithFinalFields() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      Mock2 o = new Mock2(1, new MockClass(2, "Hello"));
      MessageBufferOutput mb = new MessageBufferOutput();
      ser.serialize(o,mb);
      byte[] data = mb.getBuffer();
      Mock2 o2 = (Mock2)ser.deserialize(new MessageBufferInput(data));
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
   }
   
   com.esotericsoftware.kryo.Serializer<UUID> uuidSerializer = new com.esotericsoftware.kryo.Serializer<UUID>(true,true)
   {
      LongSerializer longSerializer = new LongSerializer();
      {
         longSerializer.setImmutable(true);
         longSerializer.setAcceptsNull(false);
      }

      @Override
      public UUID read(Kryo kryo, Input input, Class<UUID> clazz)
      {
         long mostSigBits = longSerializer.read(kryo, input, long.class);
         long leastSigBits = longSerializer.read(kryo, input, long.class);
         return new UUID(mostSigBits,leastSigBits);
      }

      @Override
      public void write(Kryo kryo, Output output, UUID uuid)
      {
         long mostSigBits = uuid.getMostSignificantBits();
         long leastSigBits = uuid.getLeastSignificantBits();
         longSerializer.write(kryo, output, mostSigBits);
         longSerializer.write(kryo, output, leastSigBits);
      }
   };
   
   KryoOptimizer defaultMock3Optimizer = new KryoOptimizer()
   {
      @Override
      public void preRegister(Kryo kryo){ }
      
      @Override
      public void postRegister(Kryo kryo)
      {
         com.esotericsoftware.kryo.Registration reg = kryo.getRegistration(UUID.class);
         reg.setSerializer(uuidSerializer);
      }
   };
   
   @Test
   public void testChildClassSerialization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>(defaultMock3Optimizer);
      
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      MessageBufferOutput mb = new MessageBufferOutput();
      ser.serialize(o,mb);
      byte[] data = mb.getBuffer();
      Mock2 o2 = (Mock2)ser.deserialize(new MessageBufferInput(data));
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
   }
   
   @SuppressWarnings({"rawtypes","unchecked"})
   private static byte[] serialize(Serializer ser, Object m) throws SerializationException
   {
      MessageBufferOutput mb = new MessageBufferOutput();
      ser.serialize(m, mb);
      return Arrays.copyOf(mb.getBuffer(),mb.getPosition());
   }
      
   @Test
   public void testChildClassSerializationWithRegistration() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>(defaultMock3Optimizer);
      JavaSerializer<Object> serJ = new JavaSerializer<Object>();
      KryoSerializer<Object> serR = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName(),10));
      KryoSerializer<Object> serRR = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName(),10),new Registration(Mock3.class.getName(), 11));
      KryoSerializer<Object> serRROb = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName()),new Registration(Mock3.class.getName()),new Registration(UUID.class.getName()));
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = serialize(ser,o);
      byte[] dataJ = serialize(serJ,o);
      byte[] dataR = serialize(serR,o);
      byte[] dataRR = serialize(serRR,o);
      byte[] dataRROb = serialize(serRROb,o);
      assertTrue(dataJ.length > data.length);
      assertTrue(dataR.length < data.length);
      assertTrue(dataRR.length < dataR.length);
      assertTrue(dataRROb.length == dataRR.length);
      Mock2 o2 = (Mock2)ser.deserialize(new MessageBufferInput(data));
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
      serRROb.deserialize(new MessageBufferInput(dataRROb));
   }
   
   @Test
   public void testChildClassSerializationWithRegistrationAndOptimization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>(defaultMock3Optimizer);
      JavaSerializer<Object> serJ = new JavaSerializer<Object>();
      KryoSerializer<Object> serR = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName(),10));
      KryoSerializer<Object> serRR = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName(),10),new Registration(Mock3.class.getName(), 11));
      KryoSerializer<Object> serRROb = new KryoSerializer<Object>(defaultMock3Optimizer,new Registration(MockClass.class.getName()),new Registration(Mock3.class.getName()));
      KryoSerializer<Object> serRRO  = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10),
            new Registration(Mock3.class.getName(), 11), new Registration(UUID.class.getName(),12));
      serRRO.setKryoOptimizer(new KryoOptimizer()
      {
         @Override
         public void preRegister(Kryo kryo)
         {
            kryo.setRegistrationRequired(true);
         }

         @Override
         public void postRegister(Kryo kryo)
         {
            @SuppressWarnings("unchecked")
            FieldSerializer<MockClass> mockClassSer = (FieldSerializer<MockClass>)kryo.getSerializer(MockClass.class);
            mockClassSer.setFieldsCanBeNull(false);
            @SuppressWarnings("unchecked")
            FieldSerializer<Mock2> mock2Ser = (FieldSerializer<Mock2>)kryo.getSerializer(MockClass.class);
            mock2Ser.setFixedFieldTypes(true);
            mock2Ser.setFieldsCanBeNull(false);

            com.esotericsoftware.kryo.Registration reg = kryo.getRegistration(UUID.class);
            reg.setSerializer(uuidSerializer);
         }
      });
      
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = serialize(ser,o);
      byte[] dataJ = serialize(serJ,o);
      byte[] dataR = serialize(serR,o);
      byte[] dataRR = serialize(serRR,o);
      byte[] dataRROb = serialize(serRROb,o);
      byte[] dataRRO = serialize(serRRO,o);
      assertTrue(dataJ.length > data.length);
      assertTrue(dataR.length < data.length);
      assertTrue(dataRR.length < dataR.length);
      assertTrue(dataRROb.length == dataRR.length);
      assertTrue(dataRRO.length <= dataRR.length);
      Mock2 o2 = (Mock2)ser.deserialize(new MessageBufferInput(data));
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
      assertEquals(o,serR.deserialize(new MessageBufferInput(dataR)));
      assertEquals(o,serRR.deserialize(new MessageBufferInput(dataRR)));
      assertEquals(o,serRRO.deserialize(new MessageBufferInput(dataRRO)));
   }
   
   @Test
   public void testCollectionSerialization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>(defaultMock3Optimizer);
      ArrayList<Mock2> mess = new ArrayList<Mock2>();
      for (int i = 0; i < 10; i++)
      {
         if (i % 2 == 0)
            mess.add(new Mock3(i,new MockClass(-i,"Hello:" + i)));
         else
            mess.add(new Mock2(i,new MockClass(-i,"Hello:" + i)));
      }
      byte[] data = serialize(ser,mess);
      @SuppressWarnings("unchecked")
      List<Mock2> des = (List<Mock2>)ser.deserialize(new MessageBufferInput(data));
      assertEquals(mess,des);
   }
   
   @Test
   public void testMultithreadedSerialization() throws Throwable
   {
      Thread[] threads = new Thread[numThreads];
      
      final Object latch = new Object();
      final AtomicBoolean done = new AtomicBoolean(false);
      final AtomicBoolean failed = new AtomicBoolean(false);
      final KryoSerializer<MockClass> ser = new KryoSerializer<MockClass>(new Registration(MockClass.class.getName(),10));
      final AtomicBoolean[] finished = new AtomicBoolean[numThreads];
      final AtomicLong[] counts = new AtomicLong[numThreads];
      final long maxSerialize = 100000;
      
      try
      {
         for (int i = 0; i < threads.length; i++)
         {

            finished[i] = new AtomicBoolean(true);
            counts[i] = new AtomicLong(0);

            final int curIndex = i;

            Thread t = new Thread(new Runnable()
            {
               int index = curIndex;
               @Override
               public void run()
               {
                  try
                  {
                     synchronized(latch)
                     {
                        finished[index].set(false);
                        latch.wait();
                     }

                     while (!done.get())
                     {
                        MockClass o = new MockClass(index, "Hello:" + index);
                        byte[] data = serialize(ser,o);
                        MockClass dser = ser.deserialize(new MessageBufferInput(data));
                        assertEquals(o,dser);
                        counts[index].incrementAndGet();
                     }
                  }
                  catch (Throwable th)
                  {
                     failed.set(true);
                  }
                  finally
                  {
                     finished[index].set(true);
                  }
               }
            },"Kryo-Test-Thread-" + i);

            t.setDaemon(true);
            t.start();
            threads[i] = t;
         }

         // wait until all the threads have been started.
         assertTrue(TestUtils.poll(baseTimeoutMillis, finished, new TestUtils.Condition<AtomicBoolean[]>()
               {
            @Override
            public boolean conditionMet(AtomicBoolean[] o) throws Throwable
            {
               for (int i = 0; i < numThreads; i++)
                  if (o[i].get())
                     return false;
               return true;
            }
               }));

         Thread.sleep(10);

         synchronized(latch) { latch.notifyAll(); }

         // wait until so many message have been serialized
         assertTrue(TestUtils.poll(baseTimeoutMillis, counts, new TestUtils.Condition<AtomicLong[]>()
               {
            @Override
            public boolean conditionMet(AtomicLong[] cnts) throws Throwable
            {
               for (int i = 0; i < numThreads; i++)
                  if (cnts[i].get() < maxSerialize)
                     return false;
               return true;
            }

               }));
      }
      finally
      {
         done.set(true);
      }
      
      for (int i = 0; i < threads.length; i++)
         threads[i].join(baseTimeoutMillis);
      
      for (int i = 0; i < threads.length; i++)
         assertTrue(finished[i].get());
      
      assertTrue(!failed.get());
   }
   
   public static enum MockEnum
   {
      FROM("F"),
      TO("T"),
      BOTH("B");

      private String publicValue;
      
      private MockEnum(String value) { publicValue = value; }
      
      @Override
      public String toString() { return publicValue; }
   }      

   public static class Mock4
   {
      private MockEnum e;
      
      public Mock4() {}
      public Mock4(MockEnum e) { this.e = e; }
      
      public MockEnum get() { return e; }
   }
   
   @Test
   public void testEnumWithNoDefaultConstructor() throws Throwable
   {
      Mock4 o = new Mock4(MockEnum.BOTH);
      KryoSerializer<Mock4> ser = new KryoSerializer<Mock4>(new Registration(Mock4.class.getName()), new Registration(MockEnum.class.getName()));
      byte[] data = serialize(ser,o);
      Mock4 dser = ser.deserialize(new MessageBufferInput(data));
      assertTrue(o.get() == dser.get());
      assertEquals(o.get().toString(),dser.get().toString());
   }
}
