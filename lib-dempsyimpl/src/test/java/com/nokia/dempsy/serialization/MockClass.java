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

import java.io.Serializable;


/**
 * Simple mock bean for TestDefaultSerializer, 
 * only lives in it's own class to make serialization easier.
 */
@SuppressWarnings("serial")
public class MockClass implements Serializable {

   private int i;
   private String s;
   
   public MockClass()
   {
      // Miranda constructor to make Serialization happy
   }
   
   public MockClass(int i, String s)
   {
      this.i = i;
      this.s = s;
   }
   
   public void setI(int i)
   {
      this.i = i;
   }
   
   public void setS(String s)
   {
      this.s = s;
   }
   
   public int getI()
   {
      return i;
   }
   
   public String getS()
   {
      return s;
   }
   
   public boolean equals(Object other)
   {
      if (other == null)
         return false;
      
      if (! (other instanceof MockClass))
         return false;
      
      MockClass t = (MockClass)other;
      
      if (t.getI() != i)
         return false;
      
      if ((s != null && t.getS() == null)
          || (s == null && t.getS() != null)
          || (! s.equals(t.getS())))
          return false;
      
      return true;
   }
}

