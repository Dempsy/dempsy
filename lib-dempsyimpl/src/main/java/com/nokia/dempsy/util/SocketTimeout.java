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

package com.nokia.dempsy.util;

import java.net.Socket;
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
   static final Logger logger = LoggerFactory.getLogger(SocketTimeout.class);
   
   private Thread thread = null;

   private final Socket socket;
   private final long timeoutMillis;
   
   private final AtomicLong startTime = new AtomicLong(-1);
   private final AtomicBoolean done = new AtomicBoolean(false);
   
   private static AutoDisposeSingleThreadScheduler scheduler = 
         new AutoDisposeSingleThreadScheduler("Static SocketTimeout Schedule");
   
   public SocketTimeout(Socket socket, long timeoutMillis)
   {
      this.socket = socket;
      this.timeoutMillis = timeoutMillis;
      this.thread = Thread.currentThread();
      
      scheduler.schedule(this,timeoutMillis,TimeUnit.MILLISECONDS);
   }
   
   public void begin() { startTime.set(System.currentTimeMillis()); }
   
   public void end() { startTime.set(0); }
   
   public void stop() 
   {
      done.set(true);
   }
   
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
      logger.warn("SocketTimeout has expired and will be interupting thread " + thread.getName());

      // we're going to kill the socket.
      try { thread.interrupt(); }
      catch (Throwable th) { logger.error("Interrupt failed.", th); }

      try { socket.close(); }
      catch (Throwable th) { logger.error("Couldn't close socket.",th); }
   }
}
