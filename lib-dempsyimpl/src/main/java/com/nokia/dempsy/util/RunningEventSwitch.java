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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a helper class with unique synchronization behavior. It can be used to start
 * a worker in another thread, allow the initiating thread to verify that's it's been 
 * started, and allow the early termination of that worker thread.
 */
public class RunningEventSwitch
{
   final AtomicBoolean externalIsRunning;
   final AtomicBoolean isRunning = new AtomicBoolean(false);
   final AtomicBoolean stopRunning = new AtomicBoolean(false);
   // this flag is used to hold up the calling thread's exit of this method
   //  until the worker is underway.
   final AtomicBoolean runningGate = new AtomicBoolean(false);
   
   public RunningEventSwitch(AtomicBoolean externalIsRunning) { this.externalIsRunning = externalIsRunning; }
   
   /**
    * This is called from the worker thread to notify the fact that
    * it's been started.
    */
   public void workerInitiateRun()
   {
      // this is synchronized because it's used as a condition variable 
      // along with the condition.
      stopRunning.set(false);
      isRunning.set(true);
      
      synchronized(runningGate)
      {
         runningGate.set(true);
         runningGate.notify();
      }
   }
   
   /**
    * The worker thread can use this method to check if it's been explicitly preempted.
    * @return
    */
   public boolean wasPreempted() { return stopRunning.get(); }
   
   /**
    * The worker thread should indicate that it's done in a finally clause on it's way
    * out.
    */
   public void workerStopping()
   {
      // This kicks the preemptWorkerAndWait out.
      synchronized(isRunning)
      {
         isRunning.set(false);
         isRunning.notify();
      }
   }
   
   /**
    * The main thread uses this method when it needs to preempt the worker and
    * wait for the worker to finish before continuing.
    */
   public void preemptWorkerAndWait()
   {
      // We need to see if we're already executing
      stopRunning.set(true); // kick out any running instantiation thread
      // wait for it to exit, it it's even running - also consider the overall
      //  Mp isRunning flag.
      synchronized(isRunning)
      {
         while (isRunning.get() && externalIsRunning.get())
         {
            try { isRunning.wait(); } catch (InterruptedException e) {}
         }
      }
   }
   
   /**
    * This allows the main thread to wait until the worker is started in order
    * to continue. This method only works once. It resets the flag so a second
    * call will block until another thread calls to workerInitiateRun().
    */
   public void waitForWorkerToStart()
   {
      // make sure the thread is running before we head out from the synchronized block
      synchronized(runningGate)
      {
         while (runningGate.get() == false && externalIsRunning.get())
         {
            try { runningGate.wait(); } catch (InterruptedException ie) {}
         }

         runningGate.set(false); // reset this flag
      }
   }
}

