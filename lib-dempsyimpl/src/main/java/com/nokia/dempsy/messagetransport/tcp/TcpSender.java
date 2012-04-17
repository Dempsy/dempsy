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

package com.nokia.dempsy.messagetransport.tcp;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;

public class TcpSender implements Sender
{
   private static Logger logger = LoggerFactory.getLogger(TcpSender.class);
   
   private TcpDestination destination;
   private Socket socket = null;
   private DataOutputStream dataOutputStream = null;
   
   private enum IsLocalAddress { Yes, No, Unknown };
   private IsLocalAddress isLocalAddress = IsLocalAddress.Unknown;
   
   protected TcpSender(TcpDestination destination) throws MessageTransportException
   {
      this.destination = destination;
   }
   
   @Override
   public synchronized void send(byte[] messageBytes) throws MessageTransportException
   {
      try
      {
         DataOutputStream localDataOutputStream = getDataOutputStream();

         localDataOutputStream.writeInt( messageBytes.length );
         localDataOutputStream.flush();
         localDataOutputStream.write( messageBytes );
         localDataOutputStream.flush();
      }
      catch (IOException ioe)
      {
         close();
         throw new MessageTransportException("It appears the client " + destination + " is no longer taking calls.",ioe);
      }
   }
   
   /**
    * This method is here for testing. It allows me to create a fake output stream that 
    * I can disrupt to test the behavior of network failures.
    */
   protected Socket makeSocket(TcpDestination destination) throws IOException
   {
      return new Socket(destination.inetAddress,destination.port); 
   }
   
   // this shouldn't be called without holding a lock
   private DataOutputStream getDataOutputStream() throws MessageTransportException, IOException
   {
      if ( dataOutputStream == null) // socket must also be null.
      {
         socket = makeSocket(destination);
         
         // There is a really odd circumstance (at least on Linux) where a connection 
         //  to a port in the dynamic range, while there is no listener on that port,
         //  from the same system/network interface, can result in a local port selection
         //  that's the same as the port that the connection attempt is to. In this case,
         //  for some reason the Socket instantiation (and connection) succeeds without
         //  a listener. We need to force a failure if this is the case.
         if (isLocalAddress == IsLocalAddress.Unknown)
         {
            if (socket.isBound())
            {
               InetAddress localSocketAddress = socket.getLocalAddress();
               isLocalAddress = 
                  (Arrays.equals(localSocketAddress.getAddress(),destination.inetAddress.getAddress())) ?
                        IsLocalAddress.Yes : IsLocalAddress.No;
            }
         }
         
         if (isLocalAddress == IsLocalAddress.Yes)
         {
            if (socket.getLocalPort() == destination.port)
               throw new IOException("Connection to self same port!!!");
         }

          dataOutputStream = new DataOutputStream( socket.getOutputStream() );
      }
      
      return dataOutputStream;
   }
   
   // this shouldn't be called without holding a lock
   protected void close()
   {
      if ( dataOutputStream != null) IOUtils.closeQuietly( dataOutputStream );
      dataOutputStream = null;
      
      closeQuietly(socket); 
      socket = null;
   }
   
   protected void closeQuietly(Socket socket) 
   {
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close socket failed for " + destination); 
         }
      }
   }
}
