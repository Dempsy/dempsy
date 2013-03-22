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
 * <p>This is the core abstraction for receiving messages. The client side of
 * a transport implementation (called an "Adaptor") needs to be wired to a 
 * MessageTransportListener</p>
 */
public interface Listener
{
   /**
    * <p>Method that accepts the callback for received messages. Given that the 
    * transport is responsible for managing threads, the transport will also let
    * the Listener implementation know if it should make every effort to 
    * handle the request or if it should "fail-fast."</p>
    * 
    * <p>fail-fast means that the Listener should not attempt to handle a request
    * if any blocking is required. In cases where the Listener determines there is
    * going to be a delay in processing the request it should simple return 'false'
    * as an indication that the message was not handled and allow the transport
    * implementation to deal with it.</p>
    *  
    * @param messageBytes The message bytes received
    * @throws MessageTransportException
    */
   public boolean onMessage(MessageBufferInput message, boolean failFast) throws MessageTransportException;
   
   /**
    * The transport implementation is responsible for letting the MessageTransportListener
    * know that the transport is being shut down. 
    */
   public void transportShuttingDown();
}
