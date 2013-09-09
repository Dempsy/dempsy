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
import com.nokia.dempsy.util.SocketTimeout;
import com.yammer.metrics.core.Histogram;

public class TcpSender implements Sender
{
   private static Logger logger = LoggerFactory.getLogger(TcpSender.class);
   
   private final TcpDestination destination;
   private Socket socket = null;

   private enum IsLocalAddress { Yes, No, Unknown };
   private IsLocalAddress isLocalAddress = IsLocalAddress.Unknown;
   
   private final AtomicReference<Thread> senderThread = new AtomicReference<Thread>();
   private final AtomicBoolean isSenderRunning = new AtomicBoolean(false);
   private final AtomicBoolean senderKeepRunning = new AtomicBoolean(false);
   private final StatsCollector statsCollector;
   private long timeoutMillis;
   protected SocketTimeout socketTimeout = null;
   private long maxNumberOfQueuedMessages;
   private long batchOutgoingMessagesDelayMillis = -1;
   private final int mtu;
   
   private final Histogram batching;
   
   protected BlockingQueue<byte[]> sendingQueue = new LinkedBlockingQueue<byte[]>();
   
   protected TcpSender(TcpDestination destination, StatsCollector statsCollector, 
         Histogram batching,
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
      
      this.batching = batching;
      
      this.start();
   }
   
   public long getBatchOutgoingMessagesDelayMillis() { return batchOutgoingMessagesDelayMillis; }
   
   public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

   public void setMaxNumberOfQueuedMessages(long maxNumberOfQueuedMessages) { this.maxNumberOfQueuedMessages = maxNumberOfQueuedMessages; }
   
   public final int getMtu() { return mtu; }
   
   public final Histogram getBatchingHistogram() { return batching; }
   
   @Override
   public void send(byte[] messageBytes) throws MessageTransportException
   {
      try { sendingQueue.put(messageBytes); }
      catch (InterruptedException e)
      {
         if (statsCollector != null) statsCollector.messageNotSent();
         throw new MessageTransportException("Failed to enqueue message to " + destination + ".",e);
      }
   }
   
   private final long calcDelay(long nextTimeToSend)
   {
      final long delay = nextTimeToSend - System.currentTimeMillis();
      return delay > 0 ? delay : 0;
   }
   
   // This method is called from the middle of the processing loop to correctly
   // increment the unsent count when an error occurs based on the current state
   // of the messages.
   private final void incrementMessageNotSent(final StatsCollector stats, final byte[] messageBytes,final long batchCount, final boolean batchOutgoingMessages)
   {
      if (stats != null)
      {
         if (!batchOutgoingMessages)
            stats.messageNotSent();
         else
         {
            final long count = batchCount + (messageBytes == null ? 0 : 1);
            for (long i = 0; i  < count; i++)
               stats.messageNotSent(); // increments this many times.
         }
      }
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
               
               DataOutputStream localDataOutputStream = null;
               
               while (senderKeepRunning.get())
               {
                  try
                  {
                     messageBytes = batchOutgoingMessages ? sendingQueue.poll(calcDelay(nextTimeToSend), TimeUnit.MILLISECONDS) : sendingQueue.take();
                     
                     // Before we do anything else, we need to check if the queue is overflowed. The only way
                     // the queue will have anything in it is if messageBytes != null, otherwise it's empty
                     // so there's no need to check the current size.
                     //
                     // We want to do this prior to calling getDataOutputStream because a failure there will
                     // cause an exception that skips the check and it may persist. If the failures are slow
                     // (like they are with Windows tcp stack when the connection simply isn't there) then
                     // this can overflow quickly unless we check prior to calling getDataOutputStream.
                     //
                     // maxNumberOfQueuedMessages < 0 means unbounded queue.
                     if (messageBytes != null && maxNumberOfQueuedMessages >= 0 && sendingQueue.size() > maxNumberOfQueuedMessages)
                     {
                        if (statsCollector != null) statsCollector.messageNotSent();
                        continue; // skip the rest of the loop
                     }
                     
                     // The queue is not overflowed or we wouldn't get here.

                     localDataOutputStream = null; // we want to reget the localDataOutputStream when we need it.

                     if (messageBytes == null) // this is only possible if we polled above ...
                                               //  which we only do if batchOutgoingMessages is true
                     {
                        // if rawByteCount == 0 (or, for that matter, if batchCount == 0) then we 
                        //  haven't sent anything yet and this must be the first time through this
                        //  loop. After the first time through the main send loop this cant really be 0
                        if (rawByteCount > 0)
                        {
                           localDataOutputStream = getDataOutputStream();
                           
                           socketTimeout.begin();
                           localDataOutputStream.flush(); // we must be past the time because the poll above timed out.
                           socketTimeout.end();
                           rawByteCount = 0; // reset the rawByteCount since we flushed
                           nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis;
                           if (batching != null)
                              batching.update(batchCount);
                           batchCount = 0;
                        }
                        messageBytes = sendingQueue.take(); // might as well wait for the next one.
                     }
                     
                     // Examine the above logic. Once we get here messageBytes cannot be null.
                     //   Therefore we have a message to send (or discard if we're overflowed).
                  
                     int size = messageBytes.length;
                     int curRawByteCount = size;
                     if (size > Short.MAX_VALUE)
                     {
                        size = -1;
                        curRawByteCount += 6; // this is the overhead of a short plus a long.... see below.
                     }
                     else
                        curRawByteCount += 2; // this means the length will fit in a short.

                     // This 'if' clause checks to see if the new message will overflow
                     //   the packet meaning we should flush what's there.
                     // if rawByteCount == 0 then there's nothing to flush.
                     if (rawByteCount > 0 && (rawByteCount + curRawByteCount > mtu))
                     {
                        if (localDataOutputStream == null)
                           localDataOutputStream = getDataOutputStream();

                        // we need to flush before sending.
                        socketTimeout.begin();
                        localDataOutputStream.flush();
                        socketTimeout.end();
                        rawByteCount = 0; // reset the rawByteCount since we flushed
                        if (batchOutgoingMessages)
                        {
                           nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis;
                           if (batching != null)
                              batching.update(batchCount);
                           batchCount = 0;
                        }
                     }

                     if (localDataOutputStream == null)
                        localDataOutputStream = getDataOutputStream();
                     // now localDataOutputStream cannot be null from here down.

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
                     batchCount++;

                     // If we're not batching then we need to flush this message
                     // If we are batching, but this message is bigger than the mtu
                     //   then we need to flush.
                     if (!batchOutgoingMessages || rawByteCount > mtu)
                     {
                        socketTimeout.begin();
                        localDataOutputStream.flush(); // flush individual message
                        socketTimeout.end();
                        rawByteCount = 0; // reset the rawByteCount since we flushed
                        if (batchOutgoingMessages)
                        {
                           nextTimeToSend = System.currentTimeMillis() + batchOutgoingMessagesDelayMillis;
                           if (batching != null)
                              batching.update(batchCount);
                           batchCount = 0;
                        }
                     }

                     if (statsCollector != null) statsCollector.messageSent(messageBytes);
                  }
                  catch (IOException ioe)
                  {
                     socketTimeout.end();
                     incrementMessageNotSent(statsCollector,messageBytes,batchCount,batchOutgoingMessages);                     
                     close();
                     logger.warn("It appears the client " + destination + " is no longer taking calls.",ioe);
                  }
                  catch (InterruptedException ie)
                  {
                     if (socketTimeout != null)
                        socketTimeout.end();
                     incrementMessageNotSent(statsCollector,messageBytes,batchCount,batchOutgoingMessages);                     
                     if (senderKeepRunning.get()) // if we're supposed to be running still, then we're not shutting down. Not sure why we reset.
                        logger.warn("Sending data to " + destination + " was interrupted for no good reason.",ie);
                  }
                  catch (Throwable th)
                  {
                     if (socketTimeout != null)
                        socketTimeout.end();
                     incrementMessageNotSent(statsCollector,messageBytes,batchCount,batchOutgoingMessages);                     
                     logger.error("Unknown exception thrown while trying to send a message to " + destination);
                  }
               }
            }
            finally
            {
               senderThread.set(null);
               isSenderRunning.set(false);
               if (socketTimeout != null)
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
