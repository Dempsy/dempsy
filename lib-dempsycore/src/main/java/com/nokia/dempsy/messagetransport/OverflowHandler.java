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

package com.nokia.dempsy.messagetransport;

import com.nokia.dempsy.message.MessageBufferInput;


/**
 * Some transports allow a callback when trying to send a message overflows.
 * Implementing this interface and setting the overflowhandler on the implementation
 * is the way to receive these callbacks.
 */
public interface OverflowHandler
{
   /**
    * Implement this method to receive the callback. It will be called from the
    * appropriate transport implementation when a send attempt overflows.
    * 
    * @param messageBytes - the message that was attempted to be sent.
    */
   public void overflow(MessageBufferInput messageBytes);
}
