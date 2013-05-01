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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class AutoDisposeSingleThreadScheduler
{
   private final String baseThreadName;
   private final AtomicLong pendingCalls = new AtomicLong(0);
   
   private final AtomicLong sequence = new AtomicLong(0);
   private final Runnable nameSetter = new Runnable()
   {
      @Override
      public void run() { Thread.currentThread().setName(baseThreadName + "-" + sequence.getAndIncrement()); }
   };
   
   private class RunnableProxy implements Runnable
   {
      final Runnable proxied;
      final AtomicBoolean decremented = new AtomicBoolean(false);
      
      private RunnableProxy(Runnable proxied) { this.proxied = proxied; }

      @Override
      public void run()
      {
         // running the proxied can resubmit the task ... so we dispose afterward
         try { proxied.run(); }
         finally { if (decrement() == 0) disposeOfScheduler(); }
      }
      
      private long decrement()
      {
         return decremented.getAndSet(true) ? Long.MAX_VALUE : pendingCalls.decrementAndGet();
      }
   }
   
   public class Cancelable
   {
      private final ScheduledFuture<?> future;
      private final RunnableProxy runnable;
      
      private Cancelable(RunnableProxy runnable, ScheduledFuture<?> future) { this.runnable = runnable; this.future = future; }
      public void cancel()
      {
         future.cancel(false);
         if (runnable.decrement() == 0) disposeOfScheduler();
      }
      
      public boolean isDone() { return future.isDone(); }
   }
   
   public AutoDisposeSingleThreadScheduler(String baseThreadName) { this.baseThreadName = baseThreadName; }
   
   public synchronized Cancelable schedule(final Runnable runnable, long timeout, TimeUnit units)
   {
      pendingCalls.incrementAndGet();
      RunnableProxy proxy = new RunnableProxy(runnable);
      return new Cancelable(proxy,getScheduledExecutor().schedule(proxy, timeout, units));
   }

   private ScheduledExecutorService scheduler = null;
   private synchronized final ScheduledExecutorService getScheduledExecutor()
   {
      if (scheduler == null)
      {
         scheduler = Executors.newScheduledThreadPool(1);
         if (baseThreadName != null) scheduler.execute(nameSetter);
      }
      return scheduler;
   }
   
   private synchronized final void disposeOfScheduler()
   {
      if (scheduler != null)
         scheduler.shutdown();
      scheduler = null;
   }
}
