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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.coda.StatsCollectorCoda;
import com.nokia.dempsy.util.SocketTimeout;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

public class TcpSender implements Sender
{
   private static Logger logger = LoggerFactory.getLogger(TcpSender.class);
   
   private final TcpDestination destination;
   private Socket socket = null;

   private enum IsLocalAddress { Yes, No, Unknown };
   private IsLocalAddress isLocalAddress = IsLocalAddress.Unknown;
   
   private AtomicReference<Thread> senderThread = new AtomicReference<Thread>();
   private AtomicBoolean isSenderRunning = new AtomicBoolean(false);
   private AtomicBoolean senderKeepRunning = new AtomicBoolean(false);
   private StatsCollector statsCollector = null;
   private long timeoutMillis;
   protected SocketTimeout socketTimeout = null;
   private long maxNumberOfQueuedMessages;
   private long batchOutgoingMessagesDelayMillis = -1;
   private final int mtu;
   
   private Histogram batching = null;

   protected BlockingQueue<byte[]> sendingQueue = new LinkedBlockingQueue<byte[]>();
   
   protected TcpSender(TcpDestination destination, StatsCollector statsCollector, 
         long maxNumberOfQueuedOutgoing, long socketWriteTimeoutMillis, 
         long batchOutgoingMessagesDelayMillis, int mtu) throws MessageTransportException
   {
      this.destination = destination;
      this.statsCollector = statsCollector;
      this.timeoutMillis = socketWriteTimeoutMillis;
      this.batchOutgoingMessagesDelayMillis = batchOutgoingMessagesDelayMillis;
      this.maxNumberOfQueuedMessages = maxNumberOfQueuedOutgoing;
      this.mtu = (int)(mtu * 0.9); // our mtu is 90% of the given one to leave enough room for headers
      if (this.statsCollector != null)
      {
         this.statsCollector.setMessagesOutPendingGauge(new StatsCollector.Gauge()
         {
            @Override
            public long value()
            {
               return sendingQueue.size();
            }
         });
      }
      
      if (batchOutgoingMessagesDelayMillis >= 0 && statsCollector != null && 
            StatsCollectorCoda.class.isAssignableFrom(statsCollector.getClass()))
         batching = Metrics.newHistogram(((StatsCollectorCoda)statsCollector).createName("messages-batched"));
      
      this.start();
   }
   
   public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

   public void setMaxNumberOfQueuedMessages(long maxNumberOfQueuedMessages) { this.maxNumberOfQueuedMessages = maxNumberOfQueuedMessages; }
   
   @Override
   public void send(byte[] messageBytes) throws MessageTransportException
   {
      try { sendingQueue.put(messageBytes); }
      catch (InterruptedException e)
      {
         if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
         throw new MessageTransportException("Failed to enqueue message to " + destination + ".",e);
      }
   }
   
   private final long calcDelay(long nextTimeToSend)
   {
      final long delay = System.currentTimeMillis() - nextTimeToSend;
      return delay > 0 ? delay : 0;
   }
   
   private void start()
   {
      senderThread.set(new Thread(new Runnable()
      {
         private DataOutputStream dataOutputStream = null;

         @Override
         public void run()
         {
            final boolean batchOutgoingMessages = batchOutgoingMessagesDelayMillis >= 0;
            long batchCount = 0;
            long nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis; // only used if batchOutgoingMessagesDelayMillis >= 0
            int rawByteCount = 0;
            
            byte[] messageBytes = null;
            try
            {
               isSenderRunning.set(true);
               senderKeepRunning.set(true);
               
               while (senderKeepRunning.get())
               {
                  try
                  {
                     messageBytes = batchOutgoingMessages ? sendingQueue.poll(calcDelay(nextTimeToSend), TimeUnit.MILLISECONDS) : sendingQueue.take();
                     
                     DataOutputStream localDataOutputStream = getDataOutputStream();

                     if (messageBytes == null) // this is only possible if we polled above ...
                                               //  which we only do if batchOutgoingMessagesDelayMillis >= 0
                     {
                        socketTimeout.begin();
                        localDataOutputStream.flush(); // we must be past the time because the poll above timed out.
                        socketTimeout.end();
                        rawByteCount = 0; // reset the rawByteCount since we flushed
                        nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis;
                        if (batching != null)
                           batching.update(batchCount);
                        batchCount = 0;
                        messageBytes = sendingQueue.take(); // might as well wait for the next one.
                     }
                  
                     if (maxNumberOfQueuedMessages < 0 || sendingQueue.size() <= maxNumberOfQueuedMessages)
                     {
                        int size = messageBytes.length;
                        int curRawByteCount = size;
                        if (size > Short.MAX_VALUE)
                        {
                           size = -1;
                           curRawByteCount += 6; // this is the overhead of a short plus a long.... see below.
                        }
                        else
                           curRawByteCount += 2; // this means the length will fit in a short.
                        
                        // if rawByteCount == 0 then we haven't sent anything anyway and we need to 
                        // put the entire message in a flush anyway.
                        if (rawByteCount > 0 && (rawByteCount + curRawByteCount > mtu))
                        {
                           // we need to flush before sending.
                           socketTimeout.begin();
                           localDataOutputStream.flush();
                           socketTimeout.end();
                           rawByteCount = 0; // reset the rawByteCount since we flushed
                           if (batchOutgoingMessages)
                              nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis;
                        }
                        
                        
                        //===================================================
                        // Do the write ... first the size
                        localDataOutputStream.writeShort( size );
                        rawByteCount += 2; // sizeof short
                        if (size == -1)
                        {
                           localDataOutputStream.writeInt(size = messageBytes.length);
                           rawByteCount += 4; // sizeof int
                        }
                        localDataOutputStream.write( messageBytes );
                        rawByteCount += size; // sizeof message
                        //===================================================

                        if (!batchOutgoingMessages)
                        {
                           socketTimeout.begin();
                           localDataOutputStream.flush(); // flush individual message
                           socketTimeout.end();
                           rawByteCount = 0; // reset the rawByteCount since we flushed
                           // no need to reget the time since we're not 
                        }
                        else
                           batchCount++;

                        if (statsCollector != null) statsCollector.messageSent(messageBytes);
                     }
                     else
                        if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
                  }
                  catch (IOException ioe)
                  {
                     socketTimeout.end();
                     if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
                     close();
                     logger.warn("It appears the client " + destination + " is no longer taking calls.",ioe);
                  }
                  catch (InterruptedException ie)
                  {
                     socketTimeout.end();
                     if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
                     if (senderKeepRunning.get()) // if we're supposed to be running still, then we're not shutting down. Not sure why we reset.
                        logger.warn("Sending data to " + destination + " was interrupted for no good reason.",ie);
                  }
                  catch (Throwable th)
                  {
                     socketTimeout.end();
                     if (statsCollector != null) statsCollector.messageNotSent(messageBytes);
                     logger.error("Unknown exception thrown while trying to send a message to " + destination);
                  }
               }
            }
            finally
            {
               senderThread.set(null);
               isSenderRunning.set(false);
               socketTimeout.stop();
            }
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
         
         // this ONLY be called from the run thread
         private void close()
         {
            if ( dataOutputStream != null) IOUtils.closeQuietly( dataOutputStream );
            dataOutputStream = null;
            
            closeQuietly(socket); 
            socket = null;
         }

      },"TcpSender to " + destination));
      senderThread.get().setDaemon(true);
      senderThread.get().start();
   }
   
   protected void stop()
   {
      Thread t = senderThread.get();
      senderKeepRunning.set(false);
      if (t != null)
         t.interrupt();
      
      // and just because
      closeQuietly(socket);
      if (socketTimeout != null)
         socketTimeout.stop();
   }
   
   /**
    * This method is here for testing. It allows me to create a fake output stream that 
    * I can disrupt to test the behavior of network failures.
    */
   protected Socket makeSocket(TcpDestination destination) throws IOException
   {
      return new Socket(destination.inetAddress,destination.port); 
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
         catch (Throwable th) { logger.debug("Socket close resulted in ",th); }
      }
   }
}
