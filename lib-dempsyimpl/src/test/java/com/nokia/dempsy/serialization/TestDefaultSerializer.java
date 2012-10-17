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

import org.junit.Test;

import com.nokia.dempsy.serialization.java.JavaSerializer;
import com.nokia.dempsy.serialization.kryo.KryoSerializer;
import com.nokia.dempsy.serialization.kryo.Registration;

public class TestDefaultSerializer 
{
   private static final int TEST_NUMBER = 42;
   private static final String TEST_STRING = "life, the universe and everything";
   
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
      KryoSerializer<Object> ser = new KryoSerializer<Object>();
      Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
      byte[] data = ser.serialize(o);
      byte[] dataR = serR.serialize(o);
      byte[] dataRR = serRR.serialize(o);
      assertTrue(dataR.length < data.length);
      assertTrue(dataRR.length < dataR.length);
      Mock2 o2 = (Mock2)ser.deserialize(data);
      assertEquals(1,o2.getInt());
      assertEquals(new MockClass(2, "Hello"),o2.getMockClass());
      assertTrue(o2 instanceof Mock3);
      assertEquals(1,((Mock3)o2).myI);
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
      
}
