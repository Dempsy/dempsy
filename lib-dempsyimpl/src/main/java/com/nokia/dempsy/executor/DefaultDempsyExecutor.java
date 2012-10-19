package com.nokia.dempsy.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultDempsyExecutor implements DempsyExecutor
{
   private ScheduledExecutorService schedule = null;
   private ExecutorService executor = null;
   private AtomicLong numLimited = null;
   private long maxNumWaitingLimitedTasks = -1;
   private int threadPoolSize = -1;
   private static final int minNumThreads = 4;
   
   private double m = 1.25;
   private int additionalThreads = 2;
   
   public DefaultDempsyExecutor() { }
   
   /**
    * Create a DefaultDempsyExecutor with a fixed number of threads while setting the
    * maximum number of limited tasks.
    */
   public DefaultDempsyExecutor(int threadPoolSize, int maxNumWaitingLimitedTasks) 
   {
      this.threadPoolSize = threadPoolSize;
      this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
   }
   
   /**
    * <p>Prior to calling start you can set the cores factor and additional
    * cores. Ultimately the number of threads in the pool will be given by:</p> 
    * 
    * <p>num threads = m * num cores + b</p>
    * 
    * <p>Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads</p>
    */
   public void setCoresFactor(double m){ this.m = m; }
   
   /**
    * <p>Prior to calling start you can set the cores factor and additional
    * cores. Ultimately the number of threads in the pool will be given by:</p> 
    * 
    * <p>num threads = m * num cores + b</p>
    * 
    * <p>Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads</p>
    */
   public void setAdditionalThreads(int additionalThreads){ this.additionalThreads = additionalThreads; }
   
   @Override
   public void start()
   {
      if (threadPoolSize == -1)
      {
         // figure out the number of cores.
         int cores = Runtime.getRuntime().availableProcessors();
         int cpuBasedThreadCount = (int)Math.ceil((double)cores * m) + additionalThreads; // why? I don't know. If you don't like it 
                                                                                          //   then use the other constructor
         threadPoolSize = Math.max(cpuBasedThreadCount, minNumThreads);
      }
      executor = Executors.newFixedThreadPool(threadPoolSize);
      schedule = Executors.newSingleThreadScheduledExecutor();
      numLimited = new AtomicLong(0);
      
      if (maxNumWaitingLimitedTasks < 0)
         maxNumWaitingLimitedTasks = 20 * threadPoolSize;
   }
   
   public int getMaxNumberOfQueuedLimitedTasks() { return (int)maxNumWaitingLimitedTasks; }
   
   public void setMaxNumberOfQueuedLimitedTasks(int maxNumWaitingLimitedTasks) { this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks; }
   
   public int getCurrentQueuedLimitedTasks() { return (int)numLimited.get(); }
   
   @Override
   public int getNumThreads() { return threadPoolSize; }
   
   @Override
   public void shutdown()
   {
      if (executor != null)
         executor.shutdown();
      
      if (schedule != null)
         schedule.shutdown();
   }
   
   public boolean isRunning() { return (schedule != null && executor != null) &&
         !(schedule.isShutdown() || schedule.isTerminated()) &&
         !(executor.isShutdown() || executor.isTerminated()); }
   
   @Override
   public <V> Future<V> submit(Callable<V> r) { return executor.submit(r); }

   @Override
   public <V> Future<V> submitLimited(final Rejectable<V> r)
   {
      Callable<V> task = new Callable<V>()
      {
         Rejectable<V> o = r;

         @Override
         public V call() throws Exception
         {
            long num = numLimited.decrementAndGet();
            if (num <= maxNumWaitingLimitedTasks)
               return o.call();
            o.rejected();
            return null;
         }
      };
      
      numLimited.incrementAndGet();

      Future<V> ret = executor.submit(task);
      return ret;
   }
   
   @Override
   public <V> Future<V> schedule(final Callable<V> r, long delay, TimeUnit unit)
   {
      final ProxyFuture<V> ret = new ProxyFuture<V>();
      
      // here we are going to wrap the Callable and the Future to change the 
      // submission to one of the other queues.
      ret.schedFuture = schedule.schedule(new Runnable(){
         Callable<V> callable = r;
         ProxyFuture<V> rret = ret;
         @Override
         public void run()
         {
            // now resubmit the callable we're proxying
            rret.set(submit(callable));
         }
      }, delay, unit);
      
      // proxy the return future.
      return ret;
   }

   private static class ProxyFuture<V> implements Future<V>
   {
      private volatile Future<V> ret;
      private volatile ScheduledFuture<?> schedFuture;
      
      private synchronized void set(Future<V> f)
      {
         ret = f;
         if (schedFuture.isCancelled())
            ret.cancel(true);
         this.notifyAll();
      }
      
      // called only from synchronized methods
      private Future<?> getCurrent() { return ret == null ? schedFuture : ret; }
      
      @Override
      public synchronized boolean cancel(boolean mayInterruptIfRunning){ return getCurrent().cancel(mayInterruptIfRunning); }

      @Override
      public synchronized boolean isCancelled() { return getCurrent().isCancelled(); }

      @Override
      public synchronized boolean isDone() { return ret == null ? false : ret.isDone(); }

      @Override
      public synchronized V get() throws InterruptedException, ExecutionException 
      {
         while (ret == null)
            this.wait();
         return ret.get();
      }

      @Override
      public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
      {
         long cur = System.currentTimeMillis();
         while (ret == null)
            this.wait(unit.toMillis(timeout));
         return ret.get(System.currentTimeMillis() - cur,TimeUnit.MILLISECONDS);
      }
   }
   
}
