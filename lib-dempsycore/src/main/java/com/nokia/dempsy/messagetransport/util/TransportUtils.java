package com.nokia.dempsy.messagetransport.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.monitoring.StatsCollector;

public class TransportUtils
{
   public final static void handleMessage(final byte[] messageBytes, final DempsyExecutor executor,
         final Listener messageTransportListener, final boolean failFast, final OverflowHandler overflowHandler, 
         final StatsCollector statsCollector, final Logger logger, final String destinationString)
   {
      if ( messageTransportListener != null)
      {
         try
         {
            final byte[] pass = messageBytes;
            executor.submitLimited(new DempsyExecutor.Rejectable<Object>()
            {
               byte[] message = pass;

               @Override
               public Object call() throws Exception
               {
                  boolean messageSuccess = messageTransportListener.onMessage( message, failFast );
                  if (overflowHandler != null && !messageSuccess)
                     overflowHandler.overflow(message);
                  return null;
               }

               @Override
               public void rejected()
               {
                  if (statsCollector != null)
                     statsCollector.messageDiscarded(message);
               }
            });

         }
         catch (Throwable se)
         {
            String messageAsString;
            try { messageAsString = (messageBytes == null ? "null" : messageBytes.toString()); } catch (Throwable th) { messageAsString = "(failed to convert message to a string)"; }
            logger.error("Unexpected listener exception on adaptor for " + destinationString +
                  " trying to process a message of type " + messageBytes.getClass().getSimpleName() + " with a value of " +
                  messageAsString + " using listener " + messageTransportListener.getClass().getSimpleName(), se);
         }
      }
   }

   public final static void closeQuietly(final Socket closable) 
   {
      closeQuietly(new Closeable()
      {
         @Override public void close() throws IOException { closable.close(); }
      },null,false);
   }
   
   public final static void closeQuietly(final ServerSocket closable) 
   {
      closeQuietly(new Closeable()
      {
         @Override public void close() throws IOException { closable.close(); }
      },null,false);
   }

   public final static void closeQuietly(final Closeable closable) { closeQuietly(closable,null,false); }

   public final static void closeQuietly(final Closeable closable, final Logger logger) { closeQuietly(closable,logger,false); }

   public final static void closeQuietly(final Closeable closable, final Logger logger, boolean warn)
   {
      if (closable == null) return;
      try { closable.close(); } catch (Throwable th)
      {
         if (logger != null)
         {
            if (warn)
               logger.warn("Failed to close " + SafeString.objectDescription(closable), th);
            else
               logger.debug("Failed to close " + SafeString.objectDescription(closable), th);
         }
      }
   }

}
