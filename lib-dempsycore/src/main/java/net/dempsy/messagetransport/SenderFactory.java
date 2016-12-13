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

package net.dempsy.messagetransport;

/**
 * Abstraction to create multiple sender based on destination.
 */
public interface SenderFactory
{

   public Sender getSender(Destination destination) throws MessageTransportException;
   
   /**
    * stop() must be implemented such that it doesn't throw an exception no matter what
    * but forces the stopping of any underlying resources that require stopping. Stop
    * is expected to stop Senders that it created.
    * 
    * NOTE: stop() must be idempotent.
    */
   public void stop();
   
   public void reclaim(Destination destination);
}
