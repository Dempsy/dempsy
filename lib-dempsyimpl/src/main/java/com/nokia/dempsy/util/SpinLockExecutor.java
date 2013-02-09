package com.nokia.dempsy.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class SpinLockExecutor
{
   final AtomicBoolean lock = new AtomicBoolean(false);
   
   public void execute(Runnable runnable)
   {
      while (lock.getAndSet(true)) { Thread.yield(); }
      try { runnable.run(); }
      finally { lock.set(false); }
   }
   
   public boolean execute(Runnable runnable, long timeoutMillis)
   {
      long startTime = System.currentTimeMillis();
      while (lock.getAndSet(true)) 
      {
         Thread.yield();
         if (System.currentTimeMillis() - startTime > timeoutMillis)
            return false;
      }
      try { runnable.run(); }
      finally { lock.set(false); }
      return true;
   }
   
   public boolean tryExecute(Runnable runnable)
   {
      if (!lock.getAndSet(true))
      {
         try { runnable.run(); }
         finally { lock.set(false); }
         return true;
      }
      else 
         return false;
   }
}
