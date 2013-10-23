package com.nokia.dempsy.util;

import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this class with blocking IO in order to force writes or reads to 
 * throw an interrupted or socket exception once a certain amount of time
 * has passed.
 */
public class SocketTimeout implements Runnable
{
   Logger logger = LoggerFactory.getLogger(SocketTimeout.class);
   
   private Socket socket;
   private long timeoutMillis;
   
   private AtomicLong startTime = new AtomicLong(-1);
   private Thread thread = null;
   private AtomicBoolean done = new AtomicBoolean(false);
   private boolean disrupted = false;
   
   private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   static {
      // set the single thread's name
      scheduler.execute(new Runnable()
      {
         @Override
         public void run()
         {
            Thread.currentThread().setName("Static SocketTimeout Schedule");
         }
      });
   }
   
   public SocketTimeout(Socket socket, long timeoutMillis)
   {
      this.socket = socket;
      this.timeoutMillis = timeoutMillis;
      this.thread = Thread.currentThread();
      
      scheduler.schedule(this,timeoutMillis,TimeUnit.MILLISECONDS);
   }
   
   public void begin() {  startTime.set(System.currentTimeMillis()); }
   
   public void end() { startTime.set(0); }
   
   public void stop() { done.set(true); }
   
   @Override
   public void run()
   {
      if (done.get())
         return;

      long b = startTime.get();
      if (b != 0 && System.currentTimeMillis() - b > timeoutMillis)
         disrupt();
      else
      {
         long nextTimeout = (b == 0 ? timeoutMillis : (System.currentTimeMillis() - b));
         if (nextTimeout < 0L) nextTimeout = 1;
         scheduler.schedule(this, nextTimeout + 1, TimeUnit.MILLISECONDS);
      }
   }
   
   public void disrupt()
   {
      // we're going to kill the socket.
      try { thread.interrupt(); }
      catch (Throwable th) { logger.error("Interrupt failed.", th); }

      try { socket.close(); }
      catch (Throwable th) { logger.error("Couldn't close socket.",th); }

      disrupted = true;
   }
   
   /**
    * This checks, then clears, the disrupted flag.
    */
   public boolean disrupted() 
   {
      final boolean ret = disrupted;
      disrupted = false;
      return ret;
   }
}
