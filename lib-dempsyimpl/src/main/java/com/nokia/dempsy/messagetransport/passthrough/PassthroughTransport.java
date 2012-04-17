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

package com.nokia.dempsy.messagetransport.passthrough;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;

public class PassthroughTransport implements Transport
{

   private static class PassthroughDestination implements Destination
   {
      Sender sender;
      
      PassthroughDestination(Sender sender) { this.sender = sender; }
   }
   
   @Override
   public SenderFactory createOutbound() throws MessageTransportException
   {
      return new SenderFactory()
      {
         private volatile boolean isStopped = false;

         @Override
         public Sender getSender(Destination destination) throws MessageTransportException
         {
            if (isStopped == true)
               throw new MessageTransportException("getSender called for the destination " + SafeString.valueOf(destination) + 
                     " on a stopped " + SafeString.valueOfClass(this));

            return ((PassthroughDestination)destination).sender;
         }
         
         @Override
         public void stop() { isStopped = true; }
      };
   }

   @Override
   public Receiver createInbound() throws MessageTransportException
   {
      return new Receiver()
      {
         List<Listener> listeners = new CopyOnWriteArrayList<Listener>();
         
         Sender sender = new Sender()
         {
            @Override
            public void send(byte[] messageBytes) throws MessageTransportException
            {
               for (Listener listener : listeners)
                  listener.onMessage(messageBytes, true);
            }
         };
         
         PassthroughDestination destination = new PassthroughDestination(sender);
         
         @Override
         public void setListener(Listener listener) throws MessageTransportException
         {
            listeners.add(listener);
         }
         
         @Override
         public Destination getDestination() throws MessageTransportException
         {
            return destination;
         }
         
         @Override
         public void stop() {}
         
      };
   }

   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler)
   {
      throw new UnsupportedOperationException();
   }

}
