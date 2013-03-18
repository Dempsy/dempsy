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
      synchronized(isRunning) { isRunning.set(true); }
      stopRunning.set(false);
      
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

