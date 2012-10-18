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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.serialization.java.JavaSerializer;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;
import com.nokia.dempsy.serialization.kryo.KryoSerializer;
import com.nokia.dempsy.serialization.kryo.Registration;

public class TestDefaultSerializer 
{
   private static final int TEST_NUMBER = 42;
   private static final String TEST_STRING = "life, the universe and everything";
   private static final long baseTimeoutMillis = 20000;
   private static final int numThreads = 25;
   
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
      
      byte[] d1 = ser1.serialize(o1);
      byte[] d2 = ser2.serialize(o1);
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
      byte[] data = ser1.serialize(new MockClass());
      ser2.deserialize(data);
   }

   private void runSerializer(Serializer<MockClass> serializer) throws Throwable
   {
      byte[] data = serializer.serialize(o1);
      assertNotNull(data);
      MockClass o2 = serializer.deserialize(data);
      assertNotNull(o2);
      assertEquals(o1, o2);
   }
   
   public static class Mock2
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
   
   public static class Mock3 extends Mock2
   {
      public int myI = -1;
      
      public Mock3() {}
      
      public Mock3(int i, MockClass mock)
      {
         super(i,mock);
         this.myI = i;
      }
      
      public boolean equals(Object obj)
      {
         Mock3 o = (Mock3)obj;
         return super.equals(obj) && myI == o.myI;
      }
   }
   
   @Test
   public void testKryoWithFinalFields() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      Mock2 o = new Mock2(1, new MockClass(2, "Hello"));
      byte[] data = ser.serialize(o);
      Mock2 o2 = (Mock2)ser.deserialize(data);
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
   }
   
   @Test
   public void testChildClassSerialization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = ser.serialize(o);
      Mock2 o2 = (Mock2)ser.deserialize(data);
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
   }
      
   @Test
   public void testChildClassSerializationWithRegistration() throws Throwable
   {
      KryoSerializer<Object> serR = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10));
      KryoSerializer<Object> serRR = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10),new Registration(Mock3.class.getName(), 11));
      KryoSerializer<Object> serRROb = new KryoSerializer<Object>(new Registration(MockClass.class.getName()),new Registration(Mock3.class.getName()));
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = ser.serialize(o);
      byte[] dataR = serR.serialize(o);
      byte[] dataRR = serRR.serialize(o);
      byte[] dataRROb = serRROb.serialize(o);
      assertTrue(dataR.length < data.length);
      assertTrue(dataRR.length < dataR.length);
      assertTrue(dataRROb.length == dataRR.length);
      Mock2 o2 = (Mock2)ser.deserialize(data);
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
   }
   
   @Test
   public void testChildClassSerializationWithRegistrationAndOptimization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      KryoSerializer<Object> serR = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10));
      KryoSerializer<Object> serRR = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10),new Registration(Mock3.class.getName(), 11));
      KryoSerializer<Object> serRROb = new KryoSerializer<Object>(new Registration(MockClass.class.getName()),new Registration(Mock3.class.getName()));
      KryoSerializer<Object> serRRO  = new KryoSerializer<Object>(new Registration(MockClass.class.getName(),10),new Registration(Mock3.class.getName(), 11));
      serRRO.setKryoOptimizer(new KryoOptimizer()
      {
         @Override
         public void optimize(Kryo kryo)
         {
            kryo.setRegistrationRequired(true);
            
            @SuppressWarnings("unchecked")
            FieldSerializer<MockClass> mockClassSer = (FieldSerializer<MockClass>)kryo.getSerializer(MockClass.class);
            mockClassSer.setFieldsCanBeNull(false);
            @SuppressWarnings("unchecked")
            FieldSerializer<Mock2> mock2Ser = (FieldSerializer<Mock2>)kryo.getSerializer(MockClass.class);
            mock2Ser.setFixedFieldTypes(true);
            mock2Ser.setFieldsCanBeNull(false);
         }
      });
      
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = ser.serialize(o);
      byte[] dataR = serR.serialize(o);
      byte[] dataRR = serRR.serialize(o);
      byte[] dataRROb = serRROb.serialize(o);
      byte[] dataRRO = serRRO.serialize(o);
      assertTrue(dataR.length < data.length);
      assertTrue(dataRR.length < dataR.length);
      assertTrue(dataRROb.length == dataRR.length);
      assertTrue(dataRRO.length <= dataRR.length);
      Mock2 o2 = (Mock2)ser.deserialize(data);
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
      assertEquals(o,serR.deserialize(dataR));
      assertEquals(o,serRR.deserialize(dataRR));
      assertEquals(o,serRRO.deserialize(dataRRO));
   }
   
   @Test
   public void testCollectionSerialization() throws Throwable
   {
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      ArrayList<Mock2> mess = new ArrayList<Mock2>();
      for (int i = 0; i < 10; i++)
      {
         if (i % 2 == 0)
            mess.add(new Mock3(i,new MockClass(-i,"Hello:" + i)));
         else
            mess.add(new Mock2(i,new MockClass(-i,"Hello:" + i)));
      }
      byte[] data = ser.serialize(mess);
      @SuppressWarnings("unchecked")
      List<Mock2> des = (List<Mock2>)ser.deserialize(data);
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
                     byte[] data = ser.serialize(o);
                     MockClass dser = ser.deserialize(data);
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
               public boolean conditionMet(AtomicLong[] o) throws Throwable
               {
                  for (int i = 0; i < numThreads; i++)
                     if (o[i].get() < maxSerialize)
                        return false;
                  return true;
               }
         
            }));
      
      
      done.set(true);
      for (int i = 0; i < threads.length; i++)
         threads[i].join(baseTimeoutMillis);
      
      for (int i = 0; i < threads.length; i++)
         assertTrue(finished[i].get());
      
      assertTrue(!failed.get());
   }
      
}
