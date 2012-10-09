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

package com.nokia.dempsy.cluster;

/**
 * <p>This enum represents a bit field.</p>
 * <pre>The least-significant-bit (LSB) is:
 *     0: PERSISTENT
 *     1: EPHEMERAL
 * </pre>    
 * 
 * <pre>The second LSB is:
 *     0: NON-SEQUENTIAL
 *     1: SEQUENTIAL</pre>
 */
public enum DirMode
{
   PERSISTENT (0), EPHEMERAL(1), SEQUENTIAL(2), 
   PERSISTENT_SEQUENTIAL(2),
   EPHEMERAL_SEQUENTIAL(3);
   
   private int flag;
   private DirMode(int flag)
   {
      this.flag = flag;
   }
   
   public int getFlag() { return this.flag;}
   
   public boolean isEphemeral() { return (flag & EPHEMERAL.flag) != 0; }

   public boolean isSequential() { return (flag & SEQUENTIAL.flag) != 0; }
}
