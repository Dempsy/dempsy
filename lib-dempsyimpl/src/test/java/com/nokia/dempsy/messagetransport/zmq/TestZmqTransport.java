package com.nokia.dempsy.messagetransport.zmq;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import org.junit.Test;

import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;

public class TestZmqTransport
{
   @Test
   public void testZmqServerStart() throws Throwable
   {
      ZmqTransport transport = new ZmqTransport();
      Receiver r = null;
      try
      {
         r = transport.createInbound(null);
         r.setListener(new Listener() { 
            @Override public void transportShuttingDown(){}
            @Override public boolean onMessage(byte[] messageBytes, boolean failFast) { return true; }
         });
         r.start();
      }
      finally
      {
         if (r != null) r.shutdown();
      }
   }
   
   @Test
   public void testZmqSendSimpleMessage() throws Throwable
   {
      ZmqTransport transport = new ZmqTransport();
      Receiver r = null;
      SenderFactory senderFactory = null;
      try
      {
         r = transport.createInbound(null);
      
         final AtomicReference<String> lastReceived = new AtomicReference<String>();
         r.setListener(new Listener()
         {
            @Override public void transportShuttingDown() { }

            @Override
            public boolean onMessage(byte[] messageBytes, boolean failFast) throws MessageTransportException
            {
               lastReceived.set(new String(messageBytes));
               return true;
            }
         });
         r.start();

         Destination d = r.getDestination();

         senderFactory = transport.createOutbound(null, null);
         Sender s = senderFactory.getSender(d);

         s.send("Hello".getBytes());

         Thread.sleep(1000);
         assertEquals("Hello",lastReceived.get());
      }
      finally
      {
         if (r != null) r.shutdown();
         if (senderFactory != null) senderFactory.shutdown();
      }
   }

}
