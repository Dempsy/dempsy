package com.nokia.dempsy.messagetransport.zmq;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.messagetransport.tcp.TcpDestination;
import com.nokia.dempsy.messagetransport.util.TransportUtils;
import com.nokia.dempsy.monitoring.StatsCollector;

public class ZmqTransport implements Transport
{
   private static final Logger logger = LoggerFactory.getLogger(ZmqTransport.class);
   
   private final static ZMQ.Context context = ZMQ.context(1);
   
   private static final AtomicLong interruptibleSocketSequence = new AtomicLong(0);

   private static final class InterruptibleZmqSocket
   {
      public final ZMQ.Socket interrupt;
      public final ZMQ.Socket socket;
      public final long sequence;
      public final ZMQ.Poller poller;
      public static final byte[] interruptMessage = new byte[] { 0 };
      public static final int POLL_SOCKET = 0;
      public static final int POLL_INTERRUPT = 1;
      
      public InterruptibleZmqSocket()
      {
         sequence = interruptibleSocketSequence.getAndIncrement();
         interrupt = context.socket(ZMQ.PULL);
         interrupt.bind("inproc://" + sequence);
         socket = context.socket(ZMQ.PULL);
         poller = context.poller(2);
         poller.register(socket,ZMQ.Poller.POLLIN);
         poller.register(interrupt, ZMQ.Poller.POLLIN);
      }
      
      public final void interrupt()
      {
         ZMQ.Socket isock = null;
         try
         {
            isock = context.socket(ZMQ.PUSH);
            isock.connect("inproc://" + sequence);
            isock.send(interruptMessage,0);
         }
         finally
         {
            if (isock != null)
               isock.close();
         }
      }
      
      public final void bind(String bstr) { socket.bind(bstr); }
      
      public final long poll() { return poller.poll(); }
      
      public final boolean isInterrupted() { return poller.pollin(POLL_INTERRUPT); }
      public final boolean hasData() { return poller.pollin(POLL_SOCKET); }
      
      public final byte[] recv() { return socket.recv(0); }
      public final void close() { socket.close(); interrupt.close(); }
   }

   @Override
   public SenderFactory createOutbound(DempsyExecutor executor, StatsCollector statsCollector) throws MessageTransportException
   {
      return new SenderFactory()
      {
         private final Map<Destination,ZmqSender> senders = new HashMap<Destination, ZmqSender>();
         private final AtomicBoolean isStopped = new AtomicBoolean(false);

         @Override
         public void stopDestination(Destination destination)
         {
            synchronized(senders)
            {
               ZmqSender s = senders.get(destination);
               if (s != null)
                  s.stop();
            }
         }
         
         @Override
         public void shutdown()
         {
            isStopped.set(true);
            synchronized(senders)
            {
               for (ZmqSender sender : senders.values())
                  sender.stop();
            }
         }
         
         @Override
         public Sender getSender(Destination destination) throws MessageTransportException
         {
            ZmqSender ret;
            synchronized (senders)
            {
               ret = senders.get(destination);
               if (ret == null)
               {
                  ret = new ZmqSender((TcpDestination)destination);
                  senders.put(destination,ret);
               }
            }
            return ret;
         }
      };
   }
   
   private static class ZmqSender implements Sender
   {
      final TcpDestination destination;
      final ZMQ.Socket socket = context.socket(ZMQ.PUSH);
      
      private ZmqSender(TcpDestination destination)
      {
         this.destination = destination;
         socket.connect ("tcp://" + destination.getInetAddress().getHostAddress() + ":" + destination.getPort());
      }
      
      @Override
      public synchronized void send(byte[] messageBytes) throws MessageTransportException
      {
         if (!socket.send(messageBytes,0))
            throw new MessageTransportException("Send failed. Who knows why?");
      }
      
      public void stop()
      {
         try { socket.close(); } catch (Throwable zmqe) { logger.error("Failed closing Zmq Sender to " + SafeString.valueOf(destination)); }
      }
   }

   @Override
   public Receiver createInbound(final DempsyExecutor pexecutor) throws MessageTransportException
   {
      return new Receiver()
      {
         private DempsyExecutor executor = pexecutor;
         
         private Listener listener = null;
         private int port = -1;
         private TcpDestination destination = null;
         private String destinationString = null;
         private boolean iStartedIt = false;
         
         private final AtomicReference<InterruptibleZmqSocket> socket = new AtomicReference<ZmqTransport.InterruptibleZmqSocket>(null);
         
         private Thread serverThread = null;
         private final AtomicBoolean serverKeepRunning = new AtomicBoolean(false);
         private final AtomicBoolean serverIsRunning = new AtomicBoolean(false);
         
         // This just gates double entry into the start call.
         private final AtomicBoolean started = new AtomicBoolean(false);

         @Override
         public void start() throws MessageTransportException
         {
            if (started.getAndSet(true))
               return;
            
            if (listener == null)
               throw new MessageTransportException("You must set the Listener prior to starting the Zmq Receiver.");
            
            if (executor == null)
            {
               DefaultDempsyExecutor defexecutor = new DefaultDempsyExecutor();
               defexecutor.setCoresFactor(1.0);
               defexecutor.setAdditionalThreads(1);
               defexecutor.setMaxNumberOfQueuedLimitedTasks(10000);
//               defexecutor.setUnlimited(true);
               defexecutor.setBlocking(true);
               executor = defexecutor;
               iStartedIt = true;
               executor.start();
            }
            
            serverIsRunning.set(false);
            serverKeepRunning.set(true);
            serverThread = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  InterruptibleZmqSocket isoc = new InterruptibleZmqSocket();
                  socket.set(isoc);

                  try
                  {
                     port = bindToEphemeralPort(isoc);
                     getDestination(); // sets destination and destinationString
                     serverIsRunning.set(true);
                     
                     Thread.currentThread().setName("Zmq Receiver Server for " + destinationString);
                     
                     System.out.println("starting it " + isoc.socket.getFD());
                     while (serverKeepRunning.get())
                     {
                        try
                        {
                           isoc.poll();
                           if (isoc.isInterrupted() && !serverKeepRunning.get())
                              break;
                           else if (isoc.hasData())
                           {
                              byte[] message = isoc.recv();
                              TransportUtils.handleMessage(message, executor, listener, getFailFast(), null, null, logger, destinationString);
                           }
                        }
                        catch (org.zeromq.ZMQException e)
                        {
                           // this is ok if we're done.
                           if (!serverKeepRunning.get())
                           {
                              logger.error("Zmq Receiver Server for " + destinationString + 
                                    " failed with what will no doubt be a very descriptive error. Probably something as useful as \"Failed! Duh!\"", e);
                           }
                        }
                     }
                  }
                  catch (MessageTransportException mte)
                  {
                     logger.error("Failed to start Zmq Receiver for " + destinationString);
                  }
                  finally
                  {
                     synchronized(serverIsRunning) { serverIsRunning.set(false); serverIsRunning.notifyAll(); }
                     isoc.close();
                  }
               }
            });
            serverThread.start();
            
            while (!serverIsRunning.get()) Thread.yield();
            
            started.set(true);
         }
         
         @Override
         public void shutdown()
         {
            serverKeepRunning.set(false);
            
            if (iStartedIt) executor.shutdown();
            
            synchronized(serverIsRunning) 
            {
               while (serverIsRunning.get()) 
               {
                  socket.get().interrupt();
                  try { serverIsRunning.wait(1000); } catch (InterruptedException ie) {}
               }
            }
         }
         
         @Override
         public void setStatsCollector(StatsCollector statsCollector)
         {
         }
         
         @Override
         public void setListener(Listener listener) { this.listener = listener; }
         
         @Override
         public boolean getFailFast() { return false; }
         
         @Override
         public Destination getDestination() throws MessageTransportException
         {
            if (destination != null)
               return destination;
            
            if (port <= 0)
               throw new MessageTransportException("Cannot create a Desintation from a Zmq Reveiver without starting it first.");
            
            Destination ret = TcpDestination.createNewDestinationForThisHost(port, false);
            destinationString = ret.toString();
            return ret;
         }
      };
   }

   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler)
   {
   }
   
   private final static int bindToEphemeralPort(InterruptibleZmqSocket socket) throws MessageTransportException
   {
      // find an unused ephemeral port
      int port = -1;
      for (boolean done = false; !done;)
      {
         port = -1;
         try
         {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(), 0);
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true); // this allows the server port to be bound to even if it's in TIME_WAIT
            serverSocket.bind(inetSocketAddress);
            port = serverSocket.getLocalPort();
            serverSocket.close();
            socket.bind("tcp://*:" + port);
            done = true;
         }
         catch (IOException e)
         {
            logger.error("Failed attempting to open a 0mq server socket on " + port,e);
         }
         catch (RuntimeException e)
         {
            logger.error("Failed attempting to open a 0mq server socket on " + port,e);
         }
      }
      
      if (port <= 0)
         throw new MessageTransportException("Couldn't set the tcp port for the Zmq Receiver.");
      
      return port;
   }


}
