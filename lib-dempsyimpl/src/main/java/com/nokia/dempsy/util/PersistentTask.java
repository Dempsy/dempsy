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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.nokia.dempsy.cluster.ClusterInfoWatcher;

public abstract class PersistentTask implements ClusterInfoWatcher
{
   private final Logger logger;
   private final AtomicBoolean isRunning;
   private final AutoDisposeSingleThreadScheduler dscheduler;
   private final long resetDelay;

   private boolean alreadyHere = false;
   private boolean recurseAttempt = false;
   
   private AutoDisposeSingleThreadScheduler.Cancelable currentlyWaitingOn = null;
   
   public PersistentTask(Logger logger, AtomicBoolean isStillRunningFlag, 
         AutoDisposeSingleThreadScheduler dscheduler, long resetDelayMillis)
   {
      this.logger = logger;
      this.isRunning = isStillRunningFlag;
      this.dscheduler = dscheduler;
      this.resetDelay = resetDelayMillis;
   }
   
   /**
    * This should return <code>true</code> if the underlying execute was successful.
    * It will be recalled until it does.
    */
   public abstract boolean execute() throws Throwable;
   
   @Override public void process() { executeUntilWorks(); }
   
   private synchronized void executeUntilWorks()
   {
      if (!isRunning.get())
         return;
      
      // we need to flatten out recursions. This may be called from 
      // the same thread but deeper in the call tree. Therefore, if
      // we're already here we want to exit without hitting the 
      // finally clause at the bottom. But we want to make sure
      // when we eventually hit the finally clause (with the other
      // thread or stack frame) we will attempt another time.
      if (alreadyHere)
      {
         recurseAttempt = true;
         return;
      }

      boolean retry = true;
      
      try
      {
         alreadyHere = true;
         
         // ok ... we're going to execute this now. So if we have an outstanding scheduled task we
         // need to cancel it.
         if (currentlyWaitingOn != null)
         {
            currentlyWaitingOn.cancel();
            currentlyWaitingOn = null;
         }
         
         retry = !execute();
         
         if (logger.isTraceEnabled())
            logger.trace("Managed to " + this + " with the results:" + !retry);
         
      }
      catch (Throwable th)
      {
         if (logger.isDebugEnabled())
            logger.debug("Exception while " + this);
      }
      finally
      {
         // if we never got the destinations set up then kick off a retry
         if (recurseAttempt)
            retry = true;
         
         recurseAttempt = false;
         alreadyHere = false;
         
         if (retry && isRunning.get())
         {
            currentlyWaitingOn = dscheduler.schedule(new Runnable(){
               @Override
               public void run() { executeUntilWorks(); }
            }, resetDelay, TimeUnit.MILLISECONDS);
         }
      }
   }
}
