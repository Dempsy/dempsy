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

package com.nokia.dempsy.internal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SafeString
{
   private static Logger logger = LoggerFactory.getLogger(SafeString.class);

   public static String valueOf(Object o)
   {
      try
      {
         return String.valueOf(o);
      }
      catch (Throwable th)
      {
         logger.warn("Failed to determine valueOf for given object",th);
      }
      
      return "[error]";
   }
   
   public static String valueOfClass(Object o)
   {
      try
      {
         Class<?> clazz = o == null ? null : o.getClass();
         return clazz == null ? "[null object has no class]" : clazz.getName();
      }
      catch (Throwable th)
      {
         logger.warn("Failed to determine valueOf for given object",th);
      }
      
      return "[error]";
   }
   
   public static String objectDescription(Object message)
   {
      return "\"" + SafeString.valueOf(message) + 
            (message != null ? "\" of type \"" + SafeString.valueOfClass(message) : "") + 
            "\"";
   }

}
