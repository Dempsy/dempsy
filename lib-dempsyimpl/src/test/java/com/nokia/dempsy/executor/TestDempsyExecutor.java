package com.nokia.dempsy.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.executor.DempsyExecutor.Rejectable;

public class TestDempsyExecutor
{
   private static long baseTimeoutMillis = 20000;

   DempsyExecutor executor = null;
   
   @Before
   public void initExecutor()
   {
      executor = new DefaultDempsyExecutor();
   }
   
   @After
   public void shutdownExecutor()
   {
      if (executor != null)
         executor.shutdown();
      executor = null;
   }
   
   static AtomicLong numRejected = new AtomicLong(0);
   public class Task implements Rejectable<Boolean>
   {
      Object latch = null;
      
      Task(Object latch) { this.latch = latch; }
      
      @Override
      public Boolean call() throws Exception
      {
         if (latch != null)
         {
            synchronized(latch)
            {
               latch.wait();
            }
         }
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
      
      Task task = new Task(null);
      Future<Boolean> result = executor.submit(task);
      
      assertTrue(result.get().booleanValue());
      
      result = executor.submitLimited(task);
      assertTrue(result.get().booleanValue());
   }
   
   @Test
   public void testLimited() throws Throwable
   {
      executor.start();
      
      Object latch = new Object();
      int numThreads = executor.getNumThreads();
      
      Task task = new Task(latch);
      
      // submit a limited task for every thread.
      for (int i = 0; i < numThreads; i++)
         executor.submitLimited(task);
      
      // submit twice as many.
      for (int i = 0; i < numThreads; i++)
         executor.submitLimited(task);

      // none should be rejected yet.
      assertEquals(0,numRejected.get());
      
      // now submit until they start getting rejected.
      TestUtils.poll(baseTimeoutMillis, task, new TestUtils.Condition<Task>() { @Override public boolean conditionMet(Task task) throws Throwable
         { executor.submitLimited(task); executor.submitLimited(task); synchronized(task.latch) { task.latch.notify(); } return numRejected.get() > 0;  }
      });
   }
}
