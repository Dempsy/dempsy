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

package com.nokia.dempsy.serialization.java;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.nokia.dempsy.serialization.Serializer;

public class TestDefaultSerializer 
{

   private static final int TEST_NUMBER = 42;
   private static final String TEST_STRING = "life, the universe and everything";
   
   private Serializer<MockClass> serializer = new JavaSerializer<MockClass>();
   private MockClass o1 = new MockClass(TEST_NUMBER, TEST_STRING);
   

   @Test
   public void testSerializeDeserialize() throws Exception
   {
      byte[] data = serializer.serialize(o1);
      assertNotNull(data);
      MockClass o2 = serializer.deserialize(data);
      assertNotNull(o2);
      assertEquals(o1, o2);
   }
      
}
