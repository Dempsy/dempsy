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

package net.dempsy.router;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import net.dempsy.messagetransport.Destination;

public abstract class SlotInformation implements Serializable
{
   private static final long serialVersionUID = 1L;

   private Set<Class<?>> messageClasses = new HashSet<Class<?>>();
   private Destination destination;

   public Set<Class<?>> getMessageClasses() { return messageClasses; }
   public void setMessageClasses(Collection<Class<?>> messageClasses)
   {
      this.messageClasses = new HashSet<Class<?>>();
      this.messageClasses.addAll(messageClasses);
   }
   
   public void addMessageClass(Class<?> messageClass){ this.messageClasses.add(messageClass); }
   
   public Destination getDestination() { return destination; }
   public void setDestination(Destination destination) { this.destination = destination; }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((destination == null) ? 0 : destination.hashCode());
      result = prime * result + ((messageClasses == null) ? 0 : messageClasses.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if(this == obj)
         return true;
      if(obj == null)
         return false;
      if(getClass() != obj.getClass())
         return false;
      SlotInformation other = (SlotInformation)obj;
      if(destination == null)
      {
         if(other.destination != null)
            return false;
      }
      else if(!destination.equals(other.destination))
         return false;
      if(messageClasses == null)
      {
         if(other.messageClasses != null)
            return false;
      }
      else if(!messageClasses.equals(other.messageClasses))
         return false;
      return true;
   }

}
