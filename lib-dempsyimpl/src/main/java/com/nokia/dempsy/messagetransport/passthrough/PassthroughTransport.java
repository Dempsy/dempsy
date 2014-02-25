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
import java.util.concurrent.atomic.AtomicLong;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.monitoring.StatsCollector;

/**
 * This transport is basically an inVM transport where the call-stack becomes 
 * the transport mechanism. It's primarily for testing when completely deterministic 
 * behavior is required.
 * 
 * When the 'send' call simply directly calls the {@link Listener} on the receive
 * side. As a result the thread will basically block in the 'send' until that message 
 * is handled, by the {@link Listener} in the same thread the 'send' was called in.
 * 
 * When an inVM Dempsy application is using this transport the effect is that the
 * initial {@link Adaptor}'s call to 'send' will not return until that message is
 * processed and any resulting messages are passed on to the next Mp in the processing 
 * chain and those are processed.
 */
public class PassthroughTransport implements Transport
{
   private boolean failFast = true;
   
   private static class ConstructableMessageBufferOutput extends MessageBufferOutput { public ConstructableMessageBufferOutput() { } }

   @Override
   public SenderFactory createOutbound(final StatsCollector statsCollector, String desc)
   {
      return new SenderFactory()
      {
         private volatile boolean isStopped = false;
         private StatsCollector sc = statsCollector;

         @Override
         public Sender getSender(Destination destination) throws MessageTransportException
         {
            if (isStopped == true)
               throw new MessageTransportException("getSender called for the destination " + SafeString.valueOf(destination) + 
                     " on a stopped " + SafeString.valueOfClass(this));

            PassthroughSender ret = (PassthroughSender)(((PassthroughDestination)destination).sender);
            ret.statsCollector = sc;
            return ret;
         }
         
         @Override
         public void shutdown() { isStopped = true; }
         
         @Override
         public void stopDestination(Destination destination)
         {
            ((PassthroughSender)(((PassthroughDestination)destination).sender)).isRunning = false;
         }

         @Override public MessageBufferOutput prepareMessage() { return new ConstructableMessageBufferOutput(); }
      };
   }
   
   private class PassthroughSender implements Sender
   {
      StatsCollector statsCollector = null;
      List<Listener> listeners;
      boolean isRunning = true;
      
      private PassthroughSender(List<Listener> listeners) { this.listeners = listeners; }

      @Override
      public void send(MessageBufferOutput message) throws MessageTransportException
      {
         MessageBufferInput messageBytes = new MessageBufferInput(message.getBuffer(),0,message.getPosition());
         if (!isRunning)
         {
            if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
            throw new MessageTransportException("send called on stopped PassthroughSender");
         }
         
         for (Listener listener : listeners)
         {
            if (statsCollector != null) statsCollector.messageSent(messageBytes.available());
            listener.onMessage(messageBytes, failFast);
         }
      }
   }
   
   @Override
   public Receiver createInbound(DempsyExecutor executor, String desc) throws MessageTransportException
   {
      return new Receiver()
      {
         List<Listener> listeners = new CopyOnWriteArrayList<Listener>();
         
         PassthroughDestination destination = new PassthroughDestination(new PassthroughSender(listeners));
         
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
         public boolean getFailFast() { return PassthroughTransport.this.getFailFast(); }
         
         @Override
         public void shutdown() {}
         
         @Override
         public void start() {}
         
         @Override
         public void setStatsCollector(StatsCollector statsCollector) { }
         
      };
   }

   /**
    * <p>Failfast is set to true by default. This means that if the Mp is busy 
    * that is the target for the sent message it busy, it will simply dicard
    * the message being sent.</p>
    * 
    * <p>Setting 'failFast' to 'false' allows multi-threaded {@link Adaptor}'s
    * to use the passthough in a more deterministic fashion than would otherwise
    * be allowed.</p>
    */
   public void setFailFast(boolean failFast) { this.failFast = failFast; }
   
   /**
    * <p>Failfast is set to true by default. This means that if the Mp is busy 
    * that is the target for the sent message it busy, it will simply dicard
    * the message being sent.</p>
    * 
    * <p>Setting 'failFast' to 'false' allows multi-threaded {@link Adaptor}'s
    * to use the passthough in a more deterministic fashion than would otherwise
    * be allowed.</p>
    */
   public boolean getFailFast() { return failFast; }
   
   private static class PassthroughDestination implements Destination
   {
      private final static AtomicLong destinationIdSequence = new AtomicLong(0);
      
      private Sender sender;
      private long destinationId;
      
      PassthroughDestination(Sender sender) { this.sender = sender; destinationId = destinationIdSequence.getAndIncrement(); }
      
      @Override public String toString() { return "Passthrough(" + destinationId + ")"; }
   }
   
}
