package com.nokia.dempsy.container.internal;

import java.util.concurrent.Semaphore;

public class InstanceWrapper
{
   private final Object instance;
   private final Semaphore lock = new Semaphore(1,true); // basically a mutex
   private boolean evicted = false;

   /**
    * DO NOT CALL THIS WITH NULL OR THE LOCKING LOGIC WONT WORK
    */
   public InstanceWrapper(Object o) { this.instance = o; }

   /**
    * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
    * @param block - whether or not to wait for the lock.
    * @return the instance if the lock was aquired. null otherwise.
    */
   public Object getExclusive(boolean block)
   {
      if (block)
      {
         boolean gotLock = false;
         while (!gotLock)
         {
            try { lock.acquire(); gotLock = true; } catch (InterruptedException e) { }
         }
      }
      else
      {
         if (!lock.tryAcquire())
            return null;
      }

      // if we got here we have the lock
      return instance;
   }

   /**
    * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
    * MAKE SURE YOU OWN THE LOCK IF YOU UNLOCK IT.
    */
   public void releaseLock()
   {
      lock.release();
   }

   protected boolean tryLock()
   {
      return lock.tryAcquire();
   }
   
   /**
    * This will prevent further operations on this instance. 
    * MAKE SURE YOU OWN THE LOCK BEFORE CALLING.
    */
   public void markEvicted() { evicted = true; }

   /**
    * Flag to indicate this instance has been evicted and no further operations should be enacted.
    * THIS SHOULDN'T BE CALLED WITHOUT HOLDING THE LOCK.
    */
   public boolean isEvicted() { return evicted; }

   //----------------------------------------------------------------------------
   //  Test access only!
   //----------------------------------------------------------------------------
   /**
    * ONLY CALLED FROM INTERNAL TESTS!
    */
   public Object ecnatsnIteg() { return instance; }
}

