package com.nokia.dempsy.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.executor.DempsyExecutor.Rejectable;

public class TestDempsyExecutor
{
   private static long baseTimeoutMillis = 20000;

   DefaultDempsyExecutor executor = null;
   static AtomicLong numRejected = new AtomicLong(0);
   
   @Before
   public void initExecutor()
   {
      numRejected.set(0);
      executor = new DefaultDempsyExecutor();
   }
   
   @After
   public void shutdownExecutor()
   {
      if (executor != null)
         executor.shutdown();
      executor = null;
   }
   
   public class Task implements Rejectable<Boolean>
   {
      final AtomicBoolean latch;
      final AtomicLong count;
      
      Task(AtomicBoolean latch, AtomicLong count) { this.latch = latch; this.count = count; }
      
      @Override
      public Boolean call() throws Exception
      {
         if (latch != null)
         {
            synchronized(latch)
            {
               while (!latch.get())
                  try { latch.wait(); } catch (InterruptedException ie) {}
            }
         }
         
         if (count != null) count.incrementAndGet();
         return true;
      }

      @Override
      public void rejected()
      {
         // we don't need to do anything as the Future will have "null" if this gets called
         numRejected.incrementAndGet();
      }
      
   }
   
   @Test
   public void testSimple() throws Throwable
   {
      executor.start();
      
      Task task = new Task(null,null);
      Future<Boolean> result = executor.submit(task);
      
      assertTrue(result.get().booleanValue());
      
      result = executor.submitLimited(task);
      assertTrue(result.get().booleanValue());
   }
   
   @Test
   public void testLimited() throws Throwable
   {
      executor.start();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         int numThreads = executor.getNumThreads();

         Task task = new Task(latch,null);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.submitLimited(task);

         // submit twice as many.
         for (int i = 0; i < numThreads; i++)
            executor.submitLimited(task);

         // none should be rejected yet.
         assertEquals(0,numRejected.get());

         // now submit until they start getting rejected.
         TestUtils.poll(baseTimeoutMillis, task, new TestUtils.Condition<Task>() 
         {  @Override 
            public boolean conditionMet(Task task) throws Throwable
            {
               executor.submitLimited(task); executor.submitLimited(task);executor.submitLimited(task);
               synchronized(task.latch) { task.latch.set(true); task.latch.notify(); }
               return numRejected.get() > 0;
            }
         });
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   
   @Test
   public void testExactLimited() throws Throwable
   {
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final int numThreads = executor.getNumThreads();
         final AtomicLong execCount = new AtomicLong(0);

         Task blockingTask = new Task(latch,execCount);
         Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.submitLimited(blockingTask);

         // none should be queued
         Thread.sleep(10);
         assertEquals(0,executor.getNumberPending());
         
         // now submit one more
         executor.submitLimited(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1 && executor.getNumberLimitedPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(100);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Callable<Object>() { @Override public Object call() { return null; } });

         // now the pending tasks should go to two.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(100);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.submitLimited(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.submitLimited(nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1;

         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));
         
         // we should have discarded none.
         // now the number limited pending should be the maxQueued
         Thread.sleep(10);
         assertEquals(0,numRejected.intValue());
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   

   @Test
   public void testExactLimitedOverflow() throws Throwable
   {
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final AtomicLong execCount = new AtomicLong(0);
         int numThreads = executor.getNumThreads();

         Task blockingTask = new Task(latch,execCount);
         Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.submitLimited(blockingTask);

         // none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 0;
            }
         }));

         // now submit one more
         executor.submitLimited(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1 && executor.getNumberLimitedPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(100);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Callable<Object>() { @Override public Object call() { return null; } });

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(100);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.submitLimited(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.submitLimited(nonblockingTask);

         // now if I add one more I should see exactly one discard.
         executor.submitLimited(nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
         
         // The additional one added causes one not to count since it's rejected.
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1;
         
         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));

         Thread.sleep(10);
         assertEquals(1,numRejected.intValue());

         // and eventually none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) { return executor.getNumberPending() == 0; }
         }));
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
}
