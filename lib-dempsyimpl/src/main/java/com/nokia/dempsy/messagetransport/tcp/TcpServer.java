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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.util.ReceiverIndexedDestination;
import com.nokia.dempsy.messagetransport.util.Server;

public class TcpServer extends Server
{
   private static Logger logger = LoggerFactory.getLogger(TcpServer.class);
   
   private ServerSocket serverSocket;
   protected boolean useLocalhost = false;
   protected int port = -1;
   
   private final static class ClientHolder
   {
      private final Socket clientSocket;
      private final DataInputStream dataInputStream;
      
      private ClientHolder(Socket clientSocket) throws IOException
      {
         this.clientSocket = clientSocket;
         this.dataInputStream = new DataInputStream( new BufferedInputStream( clientSocket.getInputStream() ) );
      }
   }
   
   public TcpServer() { super(logger,true); }

   @Override
   protected final ClientHolder accept() throws IOException { 
      return new ClientHolder(serverSocket.accept()); }
   
   @Override
   protected final void closeServer()
   {
      // this is a hack because the thread interrupt doesn't wake 
      // up an accept call
      closeQuietly(serverSocket);
      serverSocket = null;
   }
   
   @Override
   protected final TcpDestination getAndBindDestination() throws MessageTransportException
   {
      TcpDestination destination = TcpDestination.createNewDestinationForThisHost(port, useLocalhost);

      bind(destination);
      
      return destination;
   }
      
   /**
    * Directs the factory to create TcpDestinations using "localhost." By default
    * the factory will not do this since the use of this InetAddress from another
    * machine will not result in a connection back to the machine the TcpDestination
    * was created from.
    */
   public void setUseLocalhost(boolean useLocalhost) { this.useLocalhost = useLocalhost; }
   
   /**
    * <p>Directs the factory to create TcpDestinations indicating that the port number is dynamic.
    * It does this by setting the port to -1. This is actually the default if the port number
    * isn't set using {@code setPort()}.</p>
    * 
    * <p>This is meant to be used from a DI container and set as a property. The value passed
    * doesn't actually do anything. To use a fixed port call setPort which will undo this setting.</p>
    */
   public void setUseEphemeralPort(boolean useEphemeralPort) { this.port = -1; }
   
   /**
    * Directs the factory to create TcpDestinations providing the given port number.
    */
   public void setPort(int port) { this.port = port; }
   
   protected final void bind(TcpDestination destination) throws MessageTransportException
   {
      if (serverSocket == null || !serverSocket.isBound())
      {
         try
         {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(destination.inetAddress, destination.port < 0 ? 0 : destination.port);
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true); // this allows the server port to be bound to even if it's in TIME_WAIT
            serverSocket.bind(inetSocketAddress);
            destination.port = serverSocket.getLocalPort();
         }
         catch(IOException ioe) 
         { 
            throw new MessageTransportException("Cannot bind to port " + 
                  (destination.isEphemeral() ? "(ephemeral port)" : destination.port),ioe);
         }
      }
   }
   
   @Override
   protected final ReceiverIndexedDestination makeDestination(ReceiverIndexedDestination modelDestination, int receiverIndex)
   {
      return new TcpDestination((TcpDestination)modelDestination,receiverIndex);
   }
   
   @Override
   protected final void closeClient(Object acceptReturn, boolean fromClientThread)
   {
      ClientHolder holder = (ClientHolder)acceptReturn;
      IOUtils.closeQuietly(holder.dataInputStream);
      closeQuietly(holder);
   }
   
   @Override
   protected final byte[] readNextMessage(Object acceptReturn) throws EOFException, IOException
   {
      final ClientHolder h = (ClientHolder)acceptReturn;
      final DataInputStream dataInputStream = h.dataInputStream;

      int size = dataInputStream.readShort();
      if (size == -1)
         size = dataInputStream.readInt();
      if (size < 1) // we expect at least '1' for the receiverIndex
         // assume we have a corrupt channel
         throw new IOException("Read negative message size. Assuming a corrupt channel.");
      
      // always create a new buffer since the old buffer will be on a queue
      final byte[] rawMessage = new byte[size];
      dataInputStream.readFully(rawMessage, 0, size);
      return rawMessage;
   }

   @Override
   protected String getClientDescription(Object acceptReturn)
   {
      ClientHolder h = (ClientHolder)acceptReturn;
      try
      {
         return "(" + h.clientSocket.getInetAddress().getHostAddress() + ":" + h.clientSocket.getPort() + ")";
      }
      catch (Throwable th)
      {
         return "(Unknown Client)";
      }
   }
   
   private void closeQuietly(ClientHolder holder) 
   {
      Socket socket = holder.clientSocket;
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close socket failed for " + getClientDescription(holder), ioe); 
         }
      }
   }

   private void closeQuietly(ServerSocket socket) 
   {
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close of server socket failed for " + getThisDestination(), ioe); 
         }
      }
   }

}
