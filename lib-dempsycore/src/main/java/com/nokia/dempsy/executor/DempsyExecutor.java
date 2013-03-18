package com.nokia.dempsy.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * <p>The Threading model for Dempsy needs to work in close concert with the
 * Transport. The implementation of the DempsyExecutor should be chosen
 * along with the Transport. If, for example, the transport can handle 
 * acknowledged delivery of messages then the Executor should be able to 
 * apply 'back pressure' through blocking in the submitLimted.</p>
 */
public interface DempsyExecutor
{
   public static interface Rejectable<V> extends Callable<V>
   {
      public void rejected();
   }
   
   /**
    * Submit a Callable that is guaranteed to execute. Unlike {@link submitLimted}
    * this method acts like the {@link Callable} was added to an unbounded queue
    * and so should eventually execute.
    */
   public <V> Future<V> submit(Callable<V> r);
   
   /**
    * This method queues {@link Callable}s that can expire or have some
    * maximum number allowed. Normal message processing falls into this 
    * category since 'shedding' is the standard behavior.
    */
   public <V> Future<V> submitLimited(Rejectable<V> r);
   
   /**
    * Schedule a task to be executed at some time in the future.
    */
   public <V> Future<V> schedule(Callable<V> r, long delay, TimeUnit timeUnit);
   
   /**
    * How many pending tasks are there.
    */
   public int getNumberPending();
   
   /**
    * How many pending limited tasks are there
    */
   public int getNumberLimitedPending();
   
   /**
    * Start up the executor
    */
   public void start();
   
   /**
    * Perform a clean shutdown of the executor
    */
   public void shutdown();
   
   /**
    * This return value may not be valid prior to start().
    * @return
    */
   public int getNumThreads();
   
}
