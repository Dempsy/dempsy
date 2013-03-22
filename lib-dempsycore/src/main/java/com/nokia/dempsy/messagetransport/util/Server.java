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

package com.nokia.dempsy.messagetransport.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.MarkerFactory;

import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.messagetransport.MessageTransportException;

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
 * <p>Of course, this doesn't work. So instead this class manages all inbound
 * connections single server socket and accepts individual connections from other
 * nodes. When a transport is built using this class and it's sister class for the sending
 * side, the {@link SenderConnection}, the number of connections is minimized to one-to-one
 * between processes.</p>
 */
public abstract class Server
{
   private final Logger logger;
   private final ForwardedReceiver[] receivers = new ForwardedReceiver[maxReceivers];
   private final AtomicBoolean stopMe = new AtomicBoolean(false);
   private final Set<ClientThread> clientThreads = new HashSet<ClientThread>();
   private final boolean manageIndividualClients;
   private final Object eventLock = new Object();
   
   private volatile boolean eventSignaled = false;
   
   private ReceiverIndexedDestination destination = null;
   private Thread serverThread;
   private int numReceivers = 0;
   
   public static final int maxReceivers = 256;
   
   public static final class ReceivedMessage
   {
      public int receiverIndex;
      public MessageBufferInput message;
   }
   
   /**
    * If manageIndividualClients is 'false' then accept will never be called and so all calls that take the clientSocket
    * that's normally returned from 'accept' will receive null. In this case no client threads will be created
    * "per client." Instead, the main server thread will be the ClientThread and will call all of the client
    * processing methods (readNextMessage, closeClient, etc). getAndBindDestination will still be called.
    */
   protected Server(Logger logger, boolean manageIndividualClients) 
   {
      this.logger = logger;
      this.manageIndividualClients = manageIndividualClients;
   }
   
   protected abstract ReceiverIndexedDestination makeDestination(ReceiverIndexedDestination modelDestination, int receiverIndex);
   protected abstract Object accept() throws IOException;
   protected abstract void closeServer();
   protected abstract void closeClient(Object acceptReturn, boolean fromClientThread);
   protected abstract ReceiverIndexedDestination getAndBindDestination() throws MessageTransportException;
   protected abstract void readNextMessage(Object acceptReturn, ReceivedMessage messageToFill) throws EOFException, IOException;
   protected abstract String getClientDescription(Object acceptReturn);

   protected synchronized void start() throws MessageTransportException
   {
      if (isStarted())
         return;
      
      // this sets the destination instance
      destination = getAndBindDestination();
      
      // in case this is a restart, we want to reset the stopMe value.
      stopMe.set(false);
      
      if (manageIndividualClients)
      {
         serverThread = new Thread(new Runnable()
         {
            @Override
            public void run()
            {
               while (!stopMe.get())
               {
                  try
                  {
                     // Wait for an event one of the registered channels
                     Object clientSocket = accept();

                     // at the point we're committed to adding a new ClientThread to the set.
                     //  So we need to lock it.
                     synchronized(clientThreads)
                     {
                        // unless we're done.
                        if (!stopMe.get())
                        {
                           // This should come from a thread pool
                           ClientThread clientThread = makeNewClientThread(clientSocket);
                           Thread thread = new Thread(clientThread, "Client Handler for " + getClientDescription(clientSocket));
                           thread.setDaemon(true);
                           thread.start();

                           clientThreads.add(clientThread);
                        }
                     }

                  }
                  // This can happen if I rip the socket out from underneath the accept call. 
                  // Because accept doesn't exit with a Thread.interrupt call so closing the server
                  // socket from another thread is the only way to make this happen.
                  catch (IOException se)
                  {
                     // however, if we didn't explicitly stop the server, then there's another problem
                     if (!stopMe.get()) 
                        logger.error("Socket error on the server managing " + destination, se);
                  }
                  catch (Throwable th)
                  {
                     logger.error("Major error on the server managing " + destination, th);
                  }
               }

               // we're leaving so signal
               synchronized(eventLock)
               {
                  eventSignaled = true;
                  eventLock.notifyAll();
               }
            }
         }, "Server for " + destination);
      }
      else
      {
         serverThread = new Thread(makeNewClientThread(null), "Server for " + destination);
      }
      
      serverThread.start();
    }
   
   public synchronized void stop()
   {
      stopMe.set(true);
      
      if (serverThread != null)
         serverThread.interrupt();
      serverThread = null;

      closeServer();
      
      if (manageIndividualClients)
      {
         synchronized(clientThreads)
         {
            for (ClientThread ct : clientThreads)
               ct.stop();

            clientThreads.clear();
         }

         // now wait until the event is signaled
         synchronized(eventLock)
         {
            if (!eventSignaled)
            {
               try { eventLock.wait(500); } catch(InterruptedException e) { }// wait for 1/2 second
            }

            if (!eventSignaled)
               logger.warn("Couldn't release the socket accept for " + destination);
         }
      }
   }
   
   protected synchronized ReceiverIndexedDestination register(ForwardedReceiver receiver) throws MessageTransportException
   {
      start();
      ReceiverIndexedDestination nextDestination = makeNextDestination();
      this.receivers[nextDestination.getReceiverIndex()] = receiver;
      numReceivers++;
      return nextDestination;
   }
   
   protected synchronized void unregister(ReceiverIndexedDestination destination)
   {
      numReceivers--;
      if (numReceivers == 0)
         stop();

      this.receivers[destination.getReceiverIndex()] = null;
   }
   
   protected synchronized boolean isStarted()
   {
      return serverThread != null;
   }
   
   private ReceiverIndexedDestination makeNextDestination() throws MessageTransportException
   {
      // find the next open place for a receiver.
      int receiverIndex = -1;
      for (receiverIndex = 0; receiverIndex < maxReceivers; receiverIndex++)
      {
         if (receivers[receiverIndex] == null)
            break;
      }
      
      if (receiverIndex >= maxReceivers)
         throw new MessageTransportException("Maximum number of receiver reached.");
      
      return makeDestination(destination,receiverIndex);
   }
   
   // protected to be overridden in tests
   protected ClientThread makeNewClientThread(Object clientSocket)
   {
      return new ClientThread(clientSocket);
   }
   
   protected final ReceiverIndexedDestination getThisDestination() { return destination; }
   
   protected class ClientThread implements Runnable
   {
      private Object clientSocket;
      private Thread thisThread;
      protected AtomicBoolean stopClient = new AtomicBoolean(false);
      
      public ClientThread(Object clientSocket) { this.clientSocket = clientSocket; }
      
      @Override
      public void run()
      {
         thisThread = Thread.currentThread();
         Exception clientIsApparentlyGone = null;
         final ReceivedMessage receivedMessage = new ReceivedMessage();
         
         try
         {
            // When we're not managing individual clients (manageIndividualClients = false) we need to
            // make it impossible to leave this loop unless we're shutting down. The reason is, this
            // "client" loop takes the place of the "server" loop when we're not managing individual 
            // clients ... there's just one message reading loop.
            do
            {
               while (true)
               {
                  try
                  {
                     int receiverToCall = -1;
                     try
                     {
                        readNextMessage(clientSocket,receivedMessage);
                        receiverToCall = receivedMessage.receiverIndex;
                     }
                     // either a problem with the socket OR a thread interruption (InterruptedIOException)
                     catch (EOFException eof)
                     {
                        clientIsApparentlyGone = eof;
                     }
                     catch (IOException ioe)
                     {
                        clientIsApparentlyGone = ioe;
                     }

                     if (clientIsApparentlyGone == null)
                     {
                        final MessageBufferInput message = receivedMessage.message;
                        if (message == null || message.available() == 0)
                        {
                           // There's no reason to print this message if the server is shutting down
                           if (logger.isDebugEnabled() && !stopMe.get())
                              logger.debug("Received a null message on destination " + destination);

                           // if we read no bytes we should just consider ourselves lucky that we
                           // escaped a blocking read.
                           break; // leave the loop.
                        }
                        ForwardedReceiver receiver = receivers[receiverToCall];
                        if (receiver == null)  // it's possible we're shutting down
                        {
                           logger.error("Message received for a mising receiver. Unless we're shutting down, this shouldn't happen.");
                           break;
                        }
                        else
                           receiver.handleMessage(receivedMessage.message);
                     }
                     else
                     {
                        // we failed ... but maybe we're just shutting down.
                        // If not, log the message.
                        if (!stopClient.get() && logger.isDebugEnabled())
                           logger.debug("Client " + getClientDescription(clientSocket) + 
                                 " has apparently disconnected from sending messages to " + 
                                 destination, clientIsApparentlyGone);
                        // assume the client socket is dead.
                        clientIsApparentlyGone = null;
                        break; // leave the loop.
                     }
                  }
                  catch (Throwable th)
                  {
                     logger.error(MarkerFactory.getMarker("FATAL"), "Completely unexpected error. This problem should be addressed because we should never make it here in the code.", th);
                     break; // leave the loop, close up.
                  }
               } // end while loop
             
            // See the comment above the 'do' statement above for why this loop is here.
            //   stopMe is an indication that the server itself is shutting down.
            } while (manageIndividualClients && !stopMe.get());
         }
         catch (Throwable ue)
         {
            logger.error("Completely unexpected error", ue);
         }
         finally
         {
            closeClient(clientSocket,true);
            
            // remove me from the client list.
            synchronized(clientThreads)
            {
               clientThreads.remove(this);
            }
         }
      }
      
      public void stop()
      {
         stopClient.set(true);
         
         // close this SOB to force a failure in the loop
         closeClient(clientSocket,false);

         // and just in case, interrupt the socket.
         if (thisThread != null)
            thisThread.interrupt();
      }
   }
   
}
