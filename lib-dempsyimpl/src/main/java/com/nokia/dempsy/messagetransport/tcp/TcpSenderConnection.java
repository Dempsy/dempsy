package com.nokia.dempsy.messagetransport.tcp;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.util.ForwardingSender.Enqueued;
import com.nokia.dempsy.messagetransport.util.ForwardingSenderFactory;
import com.nokia.dempsy.messagetransport.util.SenderConnection;
import com.nokia.dempsy.util.SocketTimeout;

public class TcpSenderConnection extends SenderConnection implements Runnable
{
   private static Logger logger = LoggerFactory.getLogger(TcpSenderConnection.class);

   private DataOutputStream dataOutputStream = null;
   
   private enum IsLocalAddress { Yes, No, Unknown };
   private IsLocalAddress isLocalAddress = IsLocalAddress.Unknown;

   private long timeoutMillis;
   protected SocketTimeout socketTimeout = null;
   
   private Socket socket = null;
   protected TcpDestination destination;
   
   protected TcpSenderConnection(final TcpDestination baseDestination, 
         final ForwardingSenderFactory factory,final boolean blocking, final long maxNumberOfQueuedOutgoing, 
         final long socketWriteTimeoutMillis, final boolean batchOutgoingMessages)
   {
      super(baseDestination.toString(), factory, blocking, maxNumberOfQueuedOutgoing, batchOutgoingMessages, logger);
      this.timeoutMillis = socketWriteTimeoutMillis;
      this.destination = baseDestination;
   }

   @Override
   protected synchronized void cleanup()
   {
      // attempt a cleanup anyway.
      closeQuietly(socket);
      if (socketTimeout != null)
         socketTimeout.stop();
   }

   public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

   @Override
   protected void doSend(final Enqueued enqueued, final boolean batch) throws IOException, InterruptedException, MessageTransportException
   {
      
      DataOutputStream localDataOutputStream = getDataOutputStream();

      final MessageBufferOutput message = enqueued.message;
      final int size = message.getPosition();
      final boolean intSize = size > Short.MAX_VALUE;
      final byte[] messageBytes = message.getBuffer();
      socketTimeout.begin();
      try
      {
         // See the TcpSenderFactory.prepareMessage where this byte is reserved
         messageBytes[0] = (byte)(enqueued.getReceiverIndex());
         if (intSize)
         {
            localDataOutputStream.writeShort(-1);
            localDataOutputStream.writeInt(size);
         }
         else
            localDataOutputStream.writeShort( size );

         localDataOutputStream.write(messageBytes,0,message.getPosition());
         if (!batch)
            localDataOutputStream.flush(); // flush individual message
      }
      finally
      {
         socketTimeout.end();
      }
   }
   
   @Override
   protected void flush() throws IOException, InterruptedException, MessageTransportException
   {
      DataOutputStream localDataOutputStream = getDataOutputStream();
      socketTimeout.begin();
      try { localDataOutputStream.flush(); }
      finally { socketTimeout.end(); }
   }
   
   // this should ONLY be called from the read thread
   private DataOutputStream getDataOutputStream() throws MessageTransportException, IOException
   {
      if ( dataOutputStream == null) // socket must also be null.
      {
         if (socketTimeout != null)
            socketTimeout.stop();
         
         socket = makeSocket(destination);
         socketTimeout = new SocketTimeout(socket, timeoutMillis);
         
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

          dataOutputStream = new DataOutputStream( new BufferedOutputStream(socket.getOutputStream(), 1024 * 1024) );
      }
      
      return dataOutputStream;
   }

   /**
    * This method is here for testing. It allows me to create a fake output stream that 
    * I can disrupt to test the behavior of network failures.
    */
   protected Socket makeSocket(TcpDestination destination) throws IOException
   {
      return new Socket(destination.inetAddress,destination.port); 
   }
   
   private void closeQuietly(Socket socket) 
   {
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close socket failed for " + destination); 
         }
         catch (Throwable th) { logger.debug("Socket close resulted in ",th); }
      }
   }

   
   // this ONLY be called from the run thread
   @Override
   protected void close()
   {
      if ( dataOutputStream != null)
      {
         try { dataOutputStream.flush(); } catch (Throwable th)
         {
//            logger.error("Failed attempting to flush last remaining messages to " + destination,th);
         }
         IOUtils.closeQuietly( dataOutputStream );
         dataOutputStream = null;
      }
      
      closeQuietly(socket); 
      socket = null;
   }
}
