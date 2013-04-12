package com.nokia.dempsy.messagetransport.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.util.ForwardingSender.Enqueued;

/**
 * <p>This is a reusable helper class for developing transports. It provides for the
 * ability to treat a destination process as a single entity even if there are multiple
 * nodes for the same application running in the process. This is critical for
 * efficient homogeneous deployment.</p>
 *
 * <p>Imagine that there's an application running 10 different clusters on 10 different
 * machines in a homogeneous deployment. That means within each process there's 
 * 10 different listeners, one for each container/node in the process.</p>
 * 
 * <p>There's 10 processes with 10 nodes each of which can potentially connect to
 * every node within each of the nine neighbors. That means 10 outgoing connections
 * for every node. Therefore 100 outgoing connections from each process.</p>
 * 
 * <p>That ends up being ~1000 incoming connections from on every receive side:
 * 10 from every other of the 9 processes.</p>
 *  
 * <p>Of course, this doesn't work. So instead this class manages all outbound
 * connections that are going to the same process through a single connection.
 * When a transport is built using this class and it's sister class for the receive
 * side, the {@link Server}, the number of connections is minimized to one-to-one.</p>
 */
public abstract class SenderConnection implements Runnable
{
   protected final Logger logger;

   private final List<Sender> senders = new ArrayList<Sender>();
   
   private final AtomicBoolean isSenderRunning = new AtomicBoolean(false);
   private final AtomicBoolean senderKeepRunning = new AtomicBoolean(false);
   private final String clientDescription;
   private final BlockingQueue<Enqueued> sendingQueue;
   protected final ForwardingSenderFactory sourceFactory;

   private Thread senderThread = null;

   private long maxNumberOfQueuedMessages;
   private boolean batchOutgoingMessages;
//   private int highWaterMark = 0;
   
   public final BlockingQueue<Enqueued> getQ() { return sendingQueue; }
   
   protected SenderConnection(String clientDescription, ForwardingSenderFactory factory,
         boolean blocking, long maxNumberOfQueuedOutgoing, boolean batchOutgoingMessages, 
         Logger logger)
   {
      this.batchOutgoingMessages = batchOutgoingMessages;
      this.maxNumberOfQueuedMessages = maxNumberOfQueuedOutgoing;
      this.logger = logger;
      this.clientDescription = clientDescription;
      this.sourceFactory = factory;
      this.sendingQueue = blocking ? new ArrayBlockingQueue<Enqueued>((int)maxNumberOfQueuedOutgoing) : new LinkedBlockingQueue<Enqueued>();
   }

   public synchronized void start(Sender sender)
   {
      if (senders.size() == 0)
      {
         senderKeepRunning.set(true);
         senderThread = new Thread(this,SafeString.valueOf(sender));
         senderThread.setDaemon(true);
         senderThread.start();
         synchronized(isSenderRunning) 
         {
            // if the sender isn't running yet, then wait for it.
            if (!isSenderRunning.get())
            {
               try { isSenderRunning.wait(10000); } catch (InterruptedException ie) {}
            }
            
            if (!isSenderRunning.get())
               logger.error("Failed to start thread for " + SafeString.valueOf(sender));
         }
      }
      
      senders.add(sender);
   }
   
   public synchronized void stop(Sender sender)
   {
      if (logger.isTraceEnabled())
         logger.trace("Stopping " + SafeString.valueOf(sender) + " from " + this);
      
      senders.remove(sender);
      if (senders.size() == 0)
      {
         if (logger.isTraceEnabled())
            logger.trace("Shutting down connection to " + clientDescription + ", " + this);

         senderKeepRunning.set(false);
         
         // poll with interrupts, for the thread to exit.
         if (senderThread != null)
         {
            for (int i = 0; i < 3000; i++)
            {
               // senderThread can be null because the lock is released in the try block 
               //  below. When that happens senderThread can be deleted, so we need to recheck
               //  it before assuming we can interrupt it. If it is null, we can assume we're
               //  done.
               if (senderThread == null)
                  break;
               
               senderThread.interrupt();
               
               // the reason we wait instead of sleep here is because stopping may require getting the 
               // monitor on 'this'. Waiting releases the monitor for the duration
               // of the wait.
               try { this.wait(1); } catch (InterruptedException ie) {}

               if (!isSenderRunning.get())
                  break;
            }
         }
         
         if (isSenderRunning.get())
            logger.error("Couldn't seem to stop the sender thread. Ignoring.");

         cleanup();
      }
   }

   protected abstract void doSend(final Enqueued message, final boolean batch) throws IOException, InterruptedException, MessageTransportException;
   protected abstract void flush() throws IOException, InterruptedException, MessageTransportException;
   protected abstract void close();
   protected abstract void cleanup();

   @Override
   public void run()
   {
      Enqueued message = null;
      try
      {
         synchronized(isSenderRunning)
         {
            isSenderRunning.set(true);
            isSenderRunning.notifyAll();
         }
         
         int ioeFailedCount = -1;
         
         while (senderKeepRunning.get())
         {
            try
            {
               message = batchOutgoingMessages ? sendingQueue.poll() : sendingQueue.take();
               
               if (message == null)
               {
                  flush();
                  ioeFailedCount = -1;
                  message = sendingQueue.take();
               }
            
               if (maxNumberOfQueuedMessages < 0 || sendingQueue.size() <= maxNumberOfQueuedMessages)
               {
                  doSend(message,batchOutgoingMessages);

                  if (!batchOutgoingMessages)
                     ioeFailedCount = -1;
                  
//                  final int curSize = sendingQueue.size();
//                  if (curSize > highWaterMark)
//                     highWaterMark = curSize;

                  message.messageSent();
               }
               else
                  message.messageNotSent();

            }
            catch (IOException ioe)
            {
               if (message != null) message.messageNotSent();
               close();
               // This can happen lots of times so let's track it
               if (ioeFailedCount == -1)
               {
                   ioeFailedCount = 0; // this will be incremented to 0
                   logger.warn("It appears the client " + clientDescription + " is no longer taking calls. This message may be supressed for a while.",ioe);
               }
               else
                   ioeFailedCount++;
            }
            catch (InterruptedException ie)
            {
               if (message != null) message.messageNotSent();
               if (senderKeepRunning.get()) // if we're supposed to be running still, then we're not shutting down. Not sure why we reset.
                  logger.warn("Sending data to " + clientDescription + " was interrupted for no good reason.",ie);
            }
            catch (Throwable th)
            {
               if (message != null) message.messageNotSent();
               logger.error("Unknown exception thrown while trying to send a message to " + clientDescription,th);
            }
            finally
            {
               sourceFactory.returnMessageBufferOutput(message.message);
            }
         }
      }
      catch (RuntimeException re) { logger.error("Unexpected Exception!",re); }
      finally
      {
         synchronized(this) { senderThread = null; }
         close();
         isSenderRunning.set(false);
//         System.out.println("High water mark for the outbound queue to " + clientDescription + " was " + highWaterMark);
      }
   }
   
   public void setMaxNumberOfQueuedMessages(long maxNumberOfQueuedMessages) { this.maxNumberOfQueuedMessages = maxNumberOfQueuedMessages; }
   
}
