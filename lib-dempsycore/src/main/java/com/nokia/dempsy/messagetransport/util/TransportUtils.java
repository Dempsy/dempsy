package com.nokia.dempsy.messagetransport.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;

import com.nokia.dempsy.internal.util.SafeString;

public class TransportUtils
{
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
