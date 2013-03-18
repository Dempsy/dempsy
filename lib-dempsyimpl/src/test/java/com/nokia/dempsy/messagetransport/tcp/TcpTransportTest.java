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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollector;

public class TcpTransportTest
{
   static Logger logger = LoggerFactory.getLogger(TcpTransportTest.class);
   private static final int numThreads = 4;
   
   private static long baseTimeoutMillis = 20000;
   private static long socketWriteTimeoutMillis = 10000;
   
//-------------------------------------------------------------------------------------
// Support code for multirun tests.
//-------------------------------------------------------------------------------------
   
   public static interface Checker
   {
      public void check(int port, boolean localhost) throws Throwable;
   }
   
   private static int failFastCalls = 0;
   private static boolean getFailFast()
   {
      failFastCalls++;
      return (failFastCalls & 0x00000001) != 0;
   }
   
   private void runAllCombinations(Checker check) throws Throwable
   {
      try
      {
         logger.debug("checking " + check + " with ephemeral port and not using \"localhost.\"");
         check.check(-1, false);
         logger.debug("checking " + check + " with port 8765 and not using \"localhost.\"");
         check.check(8765, false);
         logger.debug("checking " + check + " with ephemeral port using \"localhost.\"");
         check.check(-1, true);
         logger.debug("checking " + check + " with port 8765 using \"localhost.\"");
         check.check(8765, true);
      }
      finally
      {
         logger.debug("ending combinations for " + check);
      }
   }
   
   private void runMain(Checker check) throws Throwable
   {
      try
      {
         logger.debug("checking " + check + " with ephemeral port and not using \"localhost.\"");
         check.check(-1, false);
      }
      finally
      {
         logger.debug("ending combinations for " + check);
      }
   }
   
//-------------------------------------------------------------------------------------
// multirun tests.
//-------------------------------------------------------------------------------------
   /**
    * Just send a simple message and make sure it gets through.
    */
   final int numMessagesPerThread = 1000000;
   
   @Test
   public void transportBlockingSimpleMessage() throws Throwable
   {
      boolean oldTrackProblems = BasicStatsCollector.trackProblems;
      BasicStatsCollector.trackProblems = true;
      long oldSocketWriteTimeoutMillis = socketWriteTimeoutMillis;
      socketWriteTimeoutMillis = 100000;
      
      try
      {
         runMain(new Checker()
         {
            @Override
            public void check(int port, boolean localhost) throws Throwable
            {
               logger.trace("========= Starting");
               SenderFactory factory = null;
               TcpReceiver adaptor = null;
               StatsCollector statsCollector = new BasicStatsCollector();
               TcpServer server = null;
               final AtomicLong messageCount = new AtomicLong(0);
               final AtomicBoolean keepGoing = new AtomicBoolean(true);

               try
               {
                  //===========================================
                  // setup the sender and receiver
                  server = new TcpServer();
                  adaptor = new TcpReceiver(server,null,getFailFast());
                  adaptor.setBlocking(true);
                  adaptor.setStatsCollector(statsCollector);
                  adaptor.setListener(new Listener()
                  {
                     @Override public void shuttingDown() { }
                     @Override public boolean onMessage(byte[] messageBytes, boolean failFast) { messageCount.incrementAndGet(); return true; }
                  });
                  factory = makeSenderFactory(false,statsCollector); // distruptible sender factory

                  // always use an ephemeral port and not localhost.
                  //               if (port > 0) server.setPort(port);
                  //               if (localhost) server.setUseLocalhost(localhost);
                  //===========================================

                  adaptor.start(); // start the adaptor
                  Destination destination = adaptor.getDestination(); // get the destination

                  // send a message
                  final Sender sender = factory.getSender(destination);
                  final byte[] message = "Hello".getBytes();
                  final AtomicBoolean failed = new AtomicBoolean(false);

                  for (int i = 0; i < numThreads; i++)
                  {
                     Thread t = new Thread(new Runnable()
                     {
                        @Override  public void run()
                        {
                           try
                           {
                              for (int i = 0; i < numMessagesPerThread && keepGoing.get(); i++)
                                 sender.send(message);
                           }
                           catch(Throwable e)
                           {
                              failed.set(true);
                              throw new RuntimeException(e);
                           }
                        }
                     });
                     t.start();
                  }

                  assertFalse(failed.get());

                  TestUtils.Condition<AtomicLong> correctNumMessagesReceived = new TestUtils.Condition<AtomicLong>()
                  {
                     @Override public boolean conditionMet(AtomicLong o) { return o.get() == (numMessagesPerThread * numThreads); } 
                  };

                  long lastNumberOfMessages = -1;
                  while (messageCount.get() > lastNumberOfMessages)
                  {
                     lastNumberOfMessages = messageCount.get();
                     if (TestUtils.poll(baseTimeoutMillis, messageCount, correctNumMessagesReceived))
                        break;
                  }

                  // wait for it to be received.
                  TestUtils.poll(baseTimeoutMillis, messageCount, correctNumMessagesReceived);

                  assertEquals(numMessagesPerThread * numThreads, messageCount.get());
               }
               finally
               {
                  logger.trace("========= Stopping");

                  keepGoing.set(false);

                  if (server != null)
                     server.stop();

                  if (factory != null)
                     factory.shutdown();

                  if (adaptor != null)
                     adaptor.shutdown();

                  logger.trace("========= Finished");
               }

            }

            @Override
            public String toString() { return "transportBlockingSimpleMessage"; }
         });
      }
      finally
      {
         BasicStatsCollector.trackProblems = oldTrackProblems;
         socketWriteTimeoutMillis = oldSocketWriteTimeoutMillis;
      }
   }
   
   @Test
   public void transportSimpleMessage() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            StatsCollector statsCollector = new BasicStatsCollector();
            TcpServer server = null;
            try
            {
               //===========================================
               // setup the sender and receiver
               server = new TcpServer();
               adaptor = new TcpReceiver(server,null,getFailFast());
               adaptor.setStatsCollector(statsCollector);
               StringListener receiver = new StringListener();
               adaptor.setListener(receiver);
               factory = makeSenderFactory(false,statsCollector); // distruptible sender factory

               if (port > 0) server.setPort(port);
               if (localhost) server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               // send a message
               Sender sender = factory.getSender(destination);
               sender.send("Hello".getBytes());
               
               // wait for it to be received.
               for (long endTime = System.currentTimeMillis() + baseTimeoutMillis;
                     endTime > System.currentTimeMillis() && receiver.numMessages.get() == 0;)
                  Thread.sleep(1);
               assertEquals(1,receiver.numMessages.get());

               // verify everything came over ok.
               assertEquals(1,receiver.receivedStringMessages.size());
               assertEquals("Hello",receiver.receivedStringMessages.iterator().next());
               adaptor.shutdown();
            }
            finally
            {
               if (server != null)
                  server.stop();
               
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }
         }
         
         @Override
         public String toString() { return "testTransportSimpleMessage"; }
      });
   }
   
   static TcpReceiver makeHangingReceiver()
   {
      TcpServer server = new TcpServer()
      {
         int count = 0;
         
         protected ClientThread makeNewClientThread(Socket clientSocket) throws IOException
         {
            count++;
            return count == 1 ? new ClientThread(clientSocket)
            {
               @Override
               public void run()
               {
                  while (!stopClient.get())
                     try { Thread.sleep(1); } catch (Throwable th) {}
               }
            } : new ClientThread(clientSocket);
         }
      };
      
      return new TcpReceiver(server,null,getFailFast());
   }
   
   @Test
   public void transportSimpleMessageHungReciever() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            final byte[] message = new byte[1024 * 8];
            BasicStatsCollector statsCollector = new BasicStatsCollector();
            try
            {
               //===========================================
               // setup the sender and receiver
               adaptor = makeHangingReceiver();
               
               adaptor.setStatsCollector(statsCollector);
               final StringListener receiver = new StringListener();
               adaptor.setListener(receiver);
               factory = makeSenderFactory(false,statsCollector);

               if (port > 0) adaptor.server.setPort(port);
               if (localhost) adaptor.server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               // send a message
               final TcpSender sender = (TcpSender)factory.getSender(destination);
               sender.connection.setTimeoutMillis(100);
               
               // wait for it to fail
               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>()
               {
                  @Override
                  public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     // this should eventually fail
                     sender.send(message); // this should work
                     return o.getMessagesNotSentCount() > 0;
                  }
               }));
               
               Thread.sleep(100);

               Long numMessagesReceived = receiver.numMessages.get();
               assertTrue(TestUtils.poll(baseTimeoutMillis, numMessagesReceived, new TestUtils.Condition<Long>()
               {
                  @Override
                  public boolean conditionMet(Long o) throws Throwable
                  {
                     // this should eventually fail
                     sender.send("Hello".getBytes()); // this should work
                     return receiver.numMessages.get() > o.longValue();
                  }
               }));
            }
            finally
            {
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }

         }
         
         @Override
         public String toString() { return "transportSimpleMessageHungReciever"; }
      });
   }

   // these are for the following test.
   private byte[] receivedByteArrayMessage;
   private CountDownLatch receiveLargeMessageLatch;
   
   /**
    * Test the sending and recieving of a large message (10 meg).
    * @throws Throwable
    */
   @Test
   public void transportLargeMessage() throws Throwable
   {
      int port = -1;
      boolean localhost = false;
      SenderFactory factory = null;
      TcpReceiver adaptor = null;
      SenderFactory senderFactory = null;
      try
      {
         //===========================================
         // setup the sender and receiver
         TcpServer server = new TcpServer();
         adaptor = new TcpReceiver(server,null,getFailFast());
         factory = makeSenderFactory(false,null); // distruptible sender factory
         receiveLargeMessageLatch = new CountDownLatch(1);
         adaptor.setListener( new Listener()
         {
            @Override
            public boolean onMessage( byte[] messageBytes, boolean failfast ) throws MessageTransportException
            {
               receivedByteArrayMessage = messageBytes;
               receiveLargeMessageLatch.countDown();
               return true;
            }

            @Override
            public void shuttingDown() { }
         } );

         if (port <= 0) adaptor.server.setUseEphemeralPort(true);
         if (port > 0) adaptor.server.setPort(port);
         if (localhost) adaptor.server.setUseLocalhost(localhost);
         //===========================================

         adaptor.start(); // start the adaptor
         adaptor.start(); // double start ... just want more coverage.

         Destination destination = adaptor.getDestination(); // get the destination
         if (port > 0) adaptor.server.setPort(port);
         if (localhost) adaptor.server.setUseLocalhost(localhost);

         senderFactory = makeSenderFactory(false,null);

         int size = 1024*1024*10;
         byte[] tosend = new byte[size];
         for (int i = 0; i < size; i++)
            tosend[i] = (byte)i;

         TcpSender sender = (TcpSender)senderFactory.getSender( destination );
         sender.connection.setTimeoutMillis(100000); // extend the timeout because of the larger messages
         sender.send( tosend );

         assertTrue(receiveLargeMessageLatch.await(1,TimeUnit.MINUTES));

         assertArrayEquals( tosend, receivedByteArrayMessage );
      }
      finally
      {
         if (factory != null)
            factory.shutdown();

         if (adaptor != null)
            adaptor.shutdown();
         
         if (senderFactory != null)
            senderFactory.shutdown();

         receivedByteArrayMessage = null;
      }
   }


   /**
    * This test checks the various behaviors of a tcp transport when the 
    * client is disrupted at the 5th byte. Multiple senders are started from
    * multiple threads and ONLY ONE fails at the 5th byte. All senders should
    * be able to continue on when that happens and so the message counts should
    * increase.
    */
   @Test
   public void transportMultipleConnectionsFailedClient() throws Throwable
   {
      runMain(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            BasicStatsCollector statsCollector = new BasicStatsCollector();
            try
            {
               //===========================================
               // setup the sender and receiver
               TcpServer server = new TcpServer();
               adaptor = new TcpReceiver(server,null,getFailFast());
               adaptor.setStatsCollector(statsCollector);
               factory = makeSenderFactory(true,statsCollector); // distruptible sender factory

               if (port > 0) adaptor.server.setPort(port);
               if (localhost) adaptor.server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               //===========================================
               // Start up sender threads and save the off
               ArrayList<Thread> threadsToJoinOn = new ArrayList<Thread>();
               SenderRunnable[] senders = new SenderRunnable[numThreads];
               for (int i = 0; i < numThreads; i++)
                  threadsToJoinOn.add(i,new Thread(
                        senders[i] = new SenderRunnable(destination,i,factory),"Test Sender for " + i));

               for (Thread thread : threadsToJoinOn)
                  thread.start();
               //===========================================

               //===========================================
               // check that one sender has failed since this is disruptable.
               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>()
               {
                  @Override
                  public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     return o.getMessagesNotSentCount() > 0;
                  }
               })); 
               //===========================================

               //===========================================
               // check that ONLY one failed (the others didn't)
               Thread.sleep(10);
               assertEquals(1,statsCollector.getMessagesNotSentCount());
               //===========================================
               
               // all of the counts should increase.
               long[] curCounts = new long[numThreads];
               int i = 0;
               for (SenderRunnable sender : senders)
                  curCounts[i++] = sender.sentMessageCount.get();
               
               // ==========================================
               // go until they are all higher. The Senders should still be running
               // and sending successfully (only one failed once) so all counts should
               // be increasing.
               boolean allHigher = false;
               for (long endTime = System.currentTimeMillis() + numThreads * (baseTimeoutMillis); endTime > System.currentTimeMillis() && !allHigher;)
               {
                  allHigher = true;
                  Thread.sleep(1);
                  i = 0;
                  for (SenderRunnable sender : senders)
                     if (curCounts[i++] >= sender.sentMessageCount.get())
                        allHigher = false;
               }
               
               assertTrue(allHigher);
               // ==========================================

               // ==========================================
               // Stop the senders.
               for (SenderRunnable sender : senders)
                  sender.keepGoing.set(false);
               
               // wait until all threads are stopped
               for (Thread t : threadsToJoinOn)
                  t.join(5000);
               
               // make sure everything actually stopped.
               for (SenderRunnable sender : senders)
                  assertTrue(sender.isStopped.get());
               // ==========================================
               
            }
            finally
            {
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }
         }
         
         @Override
         public String toString() { return "transportMultipleConnectionsFailedClient"; }
      });
   }
   
   /**
    * This test sends a few messages through (and checks that they were recived)
    * and then the adaptor is stopped. This should cause all of the sender threads
    * to exit since they are set to exit on a problem.
    * 
    * This also checks that if the listener throws an exception, the adaptor continues
    * on it's merry way.
    */
   @Test
   public void transportMultipleConnectionsFailedReceiver() throws Throwable
   {
      runMain(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            BasicStatsCollector statsCollector = new BasicStatsCollector();
            try
            {
               //===========================================
               // setup the sender and receiver
               TcpServer server = new TcpServer();
               adaptor = new TcpReceiver(server,null,getFailFast());
               adaptor.setStatsCollector(statsCollector);
               StringListener receiver = new StringListener();
               adaptor.setListener(receiver);
               factory = makeSenderFactory(false,statsCollector); // not disruptible

               if (port > 0) adaptor.server.setPort(port);
               if (localhost) adaptor.server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               //===========================================
               // Start up sender threads and save the off
               ArrayList<Thread> threadsToJoinOn = new ArrayList<Thread>();
               SenderRunnable[] senders = new SenderRunnable[numThreads];
               for (int i = 0; i < numThreads; i++)
                  threadsToJoinOn.add(i,new Thread(
                        senders[i] = new SenderRunnable(destination,i,factory).stopOnNormalFailure(true),
                        "Test Sender for " + i));

               for (Thread thread : threadsToJoinOn)
                  thread.start();
               //===========================================
               
               //===========================================
               // Wait until they all send a messgage or two
               for (SenderRunnable sender : senders)
                  for (long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis;
                        endTime > System.currentTimeMillis() && sender.sentMessageCount.get() == 0;) Thread.sleep(1);
                        
               // assert that they all sent a message
               for (SenderRunnable sender : senders)
                  assertTrue(sender.sentMessageCount.get() > 0);
               //===========================================
               
               // wait until everything's been received.
               for (long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis;
                     endTime > System.currentTimeMillis() && receiver.receivedStringMessages.size() != numThreads;)
                  Thread.sleep(1);
               // that the right messages were received will be checked later.
               
               //===========================================
               // just to stretch the code coverage, make the listener throw an exception
               long numMessages = receiver.numMessages.get();
               receiver.throwThisOnce.set(new RuntimeException("Yo!"));
               for (long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis;
                     endTime > System.currentTimeMillis() && receiver.numMessages.get() <= numMessages;) Thread.sleep(1);
               assertTrue(receiver.numMessages.get() > numMessages);
               
               // now make sure we're still receiving messages by making sure the count is STILL going up.
               numMessages = receiver.numMessages.get();
               for (long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis;
                     endTime > System.currentTimeMillis() && receiver.numMessages.get() <= numMessages;) Thread.sleep(1);
               assertTrue(receiver.numMessages.get() > numMessages);
               //===========================================
               
               assertEquals(0L,statsCollector.getMessagesNotSentCount());
               
               // pull the rug out on the adaptor
               adaptor.shutdown();
               
               // wait until the total number of failed messages == 10 * numThreads
               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>()
               {
                  @Override
                  public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     return o.getMessagesNotSentCount() > 10 * numThreads;
                  }
               }));
               
               // now stop the senders.
               for (SenderRunnable sender : senders)
                  sender.keepGoing.set(false);

               //===========================================
               // wait until all threads are stopped
               for (Thread t : threadsToJoinOn)
                  t.join(numThreads * baseTimeoutMillis);
               
               // make sure everything actually stopped.
               for (SenderRunnable sender : senders)
                  assertTrue(sender.isStopped.get());
               //===========================================

               //===========================================
               // Now check to see that we recieved everything we expected.
               assertEquals(numThreads,receiver.receivedStringMessages.size());
               for (int i = 0; i < numThreads; i++)
                  assertTrue(receiver.receivedStringMessages.contains("Hello from " + i));
               //===========================================
            }
            finally
            {
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }
         }
         
         @Override
         public String toString() { return "transportMultipleConnectionsFailedWriter"; }
      });
   }
   
   @Test
   public void testBoundedOutputQueue() throws Throwable
   {
      runMain(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            final byte[] message = new byte[1024 * 8];
            BasicStatsCollector statsCollector = new BasicStatsCollector();
            try
            {
               //===========================================
               // setup the sender and receiver
               adaptor = makeHangingReceiver();
               
               adaptor.setStatsCollector(statsCollector);
               final StringListener receiver = new StringListener();
               adaptor.setListener(receiver);
               factory = makeSenderFactory(false,statsCollector);

               if (port > 0) adaptor.server.setPort(port);
               if (localhost) adaptor.server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               // send a message
               final TcpSender sender = (TcpSender)factory.getSender(destination);
               final int maxQueuedMessages = 100;
               
               sender.connection.setMaxNumberOfQueuedMessages(maxQueuedMessages);
               
               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>()
               {
                  @Override
                  public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     // this should eventually fail
                     sender.send(message); // this should work
                     return sender.connection.sendingQueue.size() > (maxQueuedMessages * 2);
                  }
               }));
               
               Thread.sleep(100);
               
               final int backup = sender.connection.sendingQueue.size();
               
               sender.connection.socketTimeout.disrupt(); // kick it.
               
               // wait for it to fail
               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>()
               {
                  @Override
                  public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     return o.getMessagesNotSentCount() > ((maxQueuedMessages * 2) - maxQueuedMessages);
                  }
               }));
               
//               Thread.sleep(100);
               
               logger.info("there are " + backup + " message backed up, and " + 
                     statsCollector.getMessagesNotSentCount() + " have been discarded.");
               
               // This cannot be determined reliably on Cloudbees which is usually too busy
//               assertTrue("backup is " + backup + " and discarded messages is " + statsCollector.getMessagesNotSentCount(), 
//                     statsCollector.getMessagesNotSentCount() < backup);

               Long numMessagesReceived = receiver.numMessages.get();
               assertTrue(TestUtils.poll(baseTimeoutMillis, numMessagesReceived, new TestUtils.Condition<Long>()
               {
                  @Override
                  public boolean conditionMet(Long o) throws Throwable
                  {
                     sender.send("Hello".getBytes()); // this should work
                     return receiver.numMessages.get() > o.longValue();
                  }
               }));
            }
            finally
            {
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }

         }
         
         @Override
         public String toString() { return "testBoundedOutputQueue"; }
      });

   }

   
//-------------------------------------------------------------------------------------
// multirun tests support
//-------------------------------------------------------------------------------------
   
   static class SenderRunnable implements Runnable
   {
      int threadInstance;
      Destination destination;
      SenderFactory senderFactory;
      
      AtomicBoolean stopOnNormalFailure = new AtomicBoolean(false);
      AtomicBoolean totalFailure = new AtomicBoolean(false);
      AtomicBoolean isStopped = new AtomicBoolean(false);
      AtomicBoolean sendFailed = new AtomicBoolean(false);
      AtomicBoolean keepGoing = new AtomicBoolean(true);
      AtomicLong sentMessageCount = new AtomicLong(0);
      
      SenderRunnable(Destination destination, int threadInstance, SenderFactory factory)
      {
         this.destination = destination;
         this.threadInstance = threadInstance;
         this.senderFactory = factory;
      }
      
      SenderRunnable stopOnNormalFailure(boolean val) { stopOnNormalFailure.set(val); return this; }

      @Override
      public void run()
      {
         boolean justGetOut = false;
         while(keepGoing.get() && !justGetOut)
         {
            try
            {
               Sender sender = senderFactory.getSender(destination);
               sender.send( ("Hello from " + threadInstance).getBytes());
               sentMessageCount.incrementAndGet();
            }
            catch(MessageTransportException e)
            {
               sendFailed.set(true);
               // this means the send got shut down.
               if (stopOnNormalFailure.get())
                  justGetOut = true; // leave the loop
            }
            catch (Throwable th)
            {
               totalFailure.set(true);
            }
            Thread.yield();
         }

         isStopped.set(true);
      }
   }

   static class StringListener implements Listener
   {
      Set<String> receivedStringMessages = new HashSet<String>();
      AtomicReference<RuntimeException> throwThisOnce = new AtomicReference<RuntimeException>();
      AtomicLong numMessages = new AtomicLong(0);
      Object latch = null;
      AtomicLong numIn = new AtomicLong(0);
      
      public StringListener() {}
      
      public StringListener(Object latch) { this.latch = latch; }
      
      @Override
      public boolean onMessage(byte[] messageBytes, boolean failfast ) throws MessageTransportException
      {
         try
         {
            numIn.incrementAndGet();
            if (latch != null)
            {
               synchronized(latch)
               {
                  try { latch.wait(); } catch (InterruptedException e) { throw new RuntimeException(e); }
               }
            }
            synchronized(this)
            {
               receivedStringMessages.add( new String( messageBytes ) );
            }
            numMessages.incrementAndGet();
            if (throwThisOnce.get() != null)
               throw throwThisOnce.getAndSet(null);
            return true;
         }
         finally
         {
            numIn.decrementAndGet();
         }
      }

      @Override
      public void shuttingDown()
      {
      }
   }

   private static class DisruptibleOutputStream extends OutputStream
   {
      OutputStream proxied;
      Socket socket;
      int dc = -1;
      int numBytesWritten = 0;

      public DisruptibleOutputStream(OutputStream proxied, Socket socket)
      {
         this.proxied = proxied;
         this.socket = socket;
         this.dc = disruptionCount;
      }

      public void write(int b) throws IOException
      {
         if (dc >= 0)
         {
            if (numBytesWritten == dc)
            {
               try { socket.close(); } catch (Throwable e) { e.printStackTrace(); }
            }
         }

         proxied.write(b);
         numBytesWritten++;
      }

      public void flush() throws IOException { proxied.flush(); }

      public void close() throws IOException { proxied.close(); }

   }

   private static int disruptionCount = 5; // passed via side effect
   private static boolean onlyOnce = true; // passed via side effect
   
   // make sure these are reset between every test
   @Before
   public void resetGlobals()
   {
      disruptionCount = 5;
      onlyOnce = true;
   }
   
   private TcpSenderFactory makeSenderFactory(boolean disruptible, StatsCollector statsCollector)
   {
      return disruptible ? 
            new TcpSenderFactory(new ConcurrentHashMap<Destination, TcpSenderConnection>(),statsCollector,-1,10000,false){
         protected TcpSender makeTcpSender(TcpDestination destination) throws MessageTransportException
         {
            TcpSenderConnection connection = 
                  new TcpSenderConnection(destination.baseDestination(), maxNumberOfQueuedOutbound, socketWriteTimeoutMillis,batchOutgoingMessages)
            {
               boolean onceOnly = onlyOnce;
               boolean didItOnceAlready = false;
               
               protected synchronized Socket makeSocket(TcpDestination destination) throws IOException
               {
                  if (!didItOnceAlready || !onceOnly)
                  {
                     didItOnceAlready = true;
                     return new Socket(destination.inetAddress,destination.port)
                     {
                        public OutputStream getOutputStream() throws IOException
                        {
                           return new DisruptibleOutputStream(super.getOutputStream(), this);
                        }
                     };
                  }
                  else // we either did it onceAlready and onceOnly was set
                     return new Socket(destination.inetAddress,destination.port);
               }
            };
            
            return new TcpSender(connection,(TcpDestination)destination,statsCollector);
         }
      } : new TcpSenderFactory(new ConcurrentHashMap<Destination, TcpSenderConnection>(),statsCollector,-1,10000,false);
   }
   
   @Test
   public void transportMTWorkerMessage() throws Throwable
   {
      runMain(new Checker()
      {
         @Override
         public void check(int port, boolean localhost) throws Throwable
         {
            SenderFactory factory = null;
            TcpReceiver adaptor = null;
            try
            {
               //===========================================
               // setup the sender and receiver
               TcpServer server = new TcpServer();
               adaptor = new TcpReceiver(server,null,getFailFast());
               final Object latch = new Object();
               BasicStatsCollector statsCollector = new BasicStatsCollector();
               adaptor.setStatsCollector(statsCollector);
               StringListener receiver = new StringListener(latch);
               adaptor.setListener(receiver);
               factory = makeSenderFactory(false,statsCollector); // distruptible sender factory

               if (port > 0) adaptor.server.setPort(port);
               if (localhost) adaptor.server.setUseLocalhost(localhost);
               //===========================================

               adaptor.start(); // start the adaptor
               Destination destination = adaptor.getDestination(); // get the destination

               // send a message
               final Sender sender = factory.getSender(destination);
               
               final int numAdaptorThreads = adaptor.executor.getNumThreads();
               
               assertTrue(numAdaptorThreads > 1);
               
               // send as many messages as there are threads.
               
               for (int i = 0; i < (((DefaultDempsyExecutor)adaptor.executor).getMaxNumberOfQueuedLimitedTasks() + adaptor.executor.getNumThreads()); i++)
                  sender.send("Hello".getBytes());
               
               // wait until all Listeners are in and all threads enqueued
               assertTrue(TestUtils.poll(baseTimeoutMillis, receiver, new TestUtils.Condition<StringListener>() 
                     { @Override public boolean conditionMet(StringListener o) { return o.numIn.get() == numAdaptorThreads; } }));

               assertTrue(TestUtils.poll(baseTimeoutMillis, ((DefaultDempsyExecutor)adaptor.executor), new TestUtils.Condition<DefaultDempsyExecutor>() 
                     { @Override public boolean conditionMet(DefaultDempsyExecutor o) { return o.getNumberLimitedPending() == o.getMaxNumberOfQueuedLimitedTasks(); } }));

               assertEquals(0,statsCollector.getDiscardedMessageCount());
               
               // we are going to poll but we are going to keep adding to the queu of tasks. So we add 2, let one go, 
               //  until we start seeing rejects. 

               assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, 
                     new TestUtils.Condition<BasicStatsCollector>() { 
                  @Override public boolean conditionMet(BasicStatsCollector o) throws Throwable
                  {
                     sender.send("Hello".getBytes());
                     sender.send("Hello".getBytes());
                     
                     synchronized(latch)
                     {
                        latch.notify(); // single exit.
                     }

                     return o.getDiscardedMessageCount() > 0;
                  }
               }));
               
               
               receiver.latch = null;
               synchronized(latch) { latch.notifyAll(); }
               
               adaptor.shutdown();
               
               synchronized(latch) { latch.notifyAll(); }
            }
            finally
            {
               if (factory != null)
                  factory.shutdown();
               
               if (adaptor != null)
                  adaptor.shutdown();
            }

         }
         
         @Override
         public String toString() { return "transportMTWorkerMessage"; }
      });
      
   }
}
