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

package com.nokia.dempsy.container;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.internal.AnnotatedMethodInvoker;
import com.nokia.dempsy.container.internal.LifecycleHelper;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.output.OutputInvoker;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;
import com.nokia.dempsy.serialization.java.JavaSerializer;

/**
 *  <p>The {@link MpContainer} manages the lifecycle of message processors for the
 *  node that it's instantiated in.</p>
 *  
 *  The container is simple in that it does no thread management. When it's called 
 *  it assumes that the transport has provided the thread that's needed 
 */
public class MpContainer implements Listener, OutputInvoker
{
   private Logger logger = LoggerFactory.getLogger(getClass());

   // these are set during configuration
   private LifecycleHelper prototype;
   private Dispatcher dispatcher;

   // Note that this is set via spring/guice.  Tests that need it,
   // should set it them selves.  Having a default leaks 8 threads per
   // instance.
   private StatsCollector statCollector;

   // default instance optionally replaced via dependency injection
   private Serializer<Object> serializer = new JavaSerializer<Object>();

   // this is used to retrieve message keys
   private AnnotatedMethodInvoker keyMethods = new AnnotatedMethodInvoker(MessageKey.class);

   // message key -> instance that handles messages with this key
   // changes to this map will be synchronized; read-only may be concurrent
   private ConcurrentHashMap<Object,InstanceWrapper> instances = new ConcurrentHashMap<Object,InstanceWrapper>();

   // Scheduler to handle eviction thread.
   private ScheduledExecutorService evictionScheduler;

   // The ClusterId is set for the sake of error messages.
   private ClusterId clusterId;

   private volatile boolean isRunning = true;

   public MpContainer(ClusterId clusterId) { this.clusterId = clusterId; }

   protected static class InstanceWrapper
   {
      private Object instance;
      private Semaphore lock = new Semaphore(1,true); // basically a mutex
      private AtomicBoolean evicted = new AtomicBoolean(false);

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

      public boolean tryLock()
      {
         return lock.tryAcquire();
      }

      /**
       * This will set the instance reference to null. MAKE SURE
       * YOU OWN THE LOCK BEFORE CALLING.
       */
      public void markPassivated() { instance = null; }

      /**
       * This will tell you if the instance reference is null. MAKE SURE
       * YOU OWN THE LOCK BEFORE CALLING.
       */
      public boolean isPassivated() { return instance == null; }


      /**
       * This will prevent further operations on this instance. 
       * MAKE SURE YOU OWN THE LOCK BEFORE CALLING.
       */
      public void markEvicted() { evicted.set(true); }

      /**
       * Flag to indicate this instance has been evicted and no further operations should be enacted.
       */
      public boolean isEvicted() { return evicted.get(); }

      //----------------------------------------------------------------------------
      //  Test access
      //----------------------------------------------------------------------------
      protected Object getInstance() { return instance; }
   }

   //----------------------------------------------------------------------------
   //  Configuration
   //----------------------------------------------------------------------------

   public void setPrototype(Object prototype) throws ContainerException
   {
      this.prototype = new LifecycleHelper(prototype);
      
      if (outputConcurrency > 0)
         setupOutputConcurrency();
   }

   public Object getPrototype()
   {
      return prototype == null ? null : prototype.getPrototype();
   }

   public LifecycleHelper getLifecycleHelper() {
      return prototype;
   }

   public Map<Object,InstanceWrapper> getInstances() {
      return instances;
   }

   public void setDispatcher(Dispatcher dispatcher)
   {
      this.dispatcher = dispatcher;
   }

   /**
    * Set the StatsCollector.  This is optional, but likely desired in
    * a production environment.  The default is to use the Coda Metrics 
    * StatsCollector with only the default JMX reporters.  Production
    * environments will likely want to export stats to a centralized
    * monitor like Graphite or Ganglia.
    */
   public void setStatCollector(StatsCollector collector)
   {
      this.statCollector = collector;
   }

   /**
    * Set the serializer.  This can be injected to change the serialization
    * scheme, but is optional.  The default serializer uses Java Serialization.
    * 
    * @param serializer
    */
   public void setSerializer(Serializer<Object> serializer) { this.serializer = serializer; }

   public Serializer<Object> getSerializer() { return this.serializer; }


   //----------------------------------------------------------------------------
   //  Monitoring / Management
   //----------------------------------------------------------------------------


   //----------------------------------------------------------------------------
   //  Operation
   //----------------------------------------------------------------------------

   /**
    *  {@inheritDoc}
    *  @return true, to acknowledge the message.
    */
   @Override
   public boolean onMessage(byte[] data, boolean fastfail)
   {
      Object message = null;
      try
      {
         message = serializer.deserialize(data);
         statCollector.messageReceived(message);
         return dispatch(message,!fastfail);
      }
      catch(SerializationException e2)
      {
         logger.warn("the container for " + clusterId + " failed to deserialize message received for " + clusterId, e2);
      }
      catch (Throwable ex)
      {
         logger.warn("the container for " + clusterId + " failed to dispatch message the following message " + 
               SafeString.objectDescription(message) + " to the message processor " + SafeString.valueOf(prototype), ex);
      }

      return false;
   }


   /**
    *  {@inheritDoc}
    */
   @Override
   public void shuttingDown()
   {
      isRunning = false;
      shutdown();
      // we don't care about the transport shutting down (currently)

   }

   public void shutdown()
   {      
      if (evictionScheduler != null)
         evictionScheduler.shutdownNow();

      // the following will close up any output executor that might be running
      setConcurrency(-1);
   }


   //----------------------------------------------------------------------------
   // Monitoring and Managmeent
   //----------------------------------------------------------------------------

   /**
    *  Returns the number of message processors controlled by this manager.
    */
   public int getProcessorCount()
   {
      return instances.size();
   }

   //----------------------------------------------------------------------------
   //  Test Hooks
   //----------------------------------------------------------------------------

   protected Dispatcher getDispatcher() { return dispatcher; }

   protected StatsCollector getStatsCollector() { return statCollector; }

   /**
    *  Returns the Message Processor that is associated with a given key,
    *  <code>null</code> if there is no such MP. Does <em>not</em> create
    *  a new MP.
    *  <p>
    *  <em>This method exists for testing; don't do anything stupid</em>
    */
   public Object getMessageProcessor(Object key)
   {
      InstanceWrapper wrapper = instances.get(key);
      return (wrapper != null) ? wrapper.getInstance() : null;
   }


   //----------------------------------------------------------------------------
   //  Internals
   //----------------------------------------------------------------------------

   // this is called directly from tests but shouldn't be accessed otherwise.
   protected boolean dispatch(Object message, boolean block) throws ContainerException {
      if (message == null)
         return false; // No. We didn't process the null message

      InstanceWrapper wrapper;
      wrapper = getInstanceForDispatch(message);

      boolean ret = false;

      // wrapper cannot be null ... look at the getInstanceForDispatch method
      boolean gotLock = false;

      if(wrapper.isEvicted()){
         logger.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype)
               + " due to eviction");
         statCollector.messageDiscarded(message);
         return ret;
      }

      try {
         Object instance = wrapper.getExclusive(block);
         if (instance != null) // null indicates we didn't get the lock
         {
            if(wrapper.isEvicted()){
               logger.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype)
                     + " due to eviction");
               statCollector.messageDiscarded(message);
               return ret;
            }

            gotLock = true;
            invokeOperation(wrapper.getInstance(), Operation.handle, message);
            ret = true;
         } else {
            if (logger.isTraceEnabled())
               logger.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
            statCollector.messageDiscarded(message);
         }
      } finally {
         if (gotLock)
            wrapper.releaseLock();
      }

      return ret;
   }

   /**
    *  Returns the instance associated with the given message, creating it if
    *  necessary. Will append the message to the instance's work queue (which
    *  may also contain an activation invocation).
    */
   protected InstanceWrapper getInstanceForDispatch(Object message) throws ContainerException
   {
      if (message == null)
         throw new ContainerException("the container for " + clusterId + " attempted to dispatch null message.");

      if (!prototype.isMessageSupported(message))
         throw new ContainerException("the container for " + clusterId + " has a prototype " + SafeString.valueOf(prototype) + 
               " that does not handle messages of class " + SafeString.valueOfClass(message));

      Object key = getKeyFromMessage(message);
      InstanceWrapper wrapper = getInstanceForKey(key);
      return wrapper;
   }

   public void evict() {
      if (!prototype.isEvictableSupported() || !isRunning)
         return;

      statCollector.evictionPassStarted();

      // we need to make a copy of the instances in order to make sure
      // the eviction check is done at once.
      Map<Object,InstanceWrapper> instancesToEvict = new HashMap<Object,InstanceWrapper>(instances.size() + 10);
      instancesToEvict.putAll(instances);

      while (instancesToEvict.size() > 0 && instances.size() > 0 && isRunning)
      {
         // store off anything that passes for later removal. This is to avoid a
         // ConcurrentModificationException.
         Set<Object> keysToRemove = new HashSet<Object>();

         for (Map.Entry<Object, InstanceWrapper> entry : instancesToEvict.entrySet())
         {
            Object key = entry.getKey();
            InstanceWrapper wrapper = entry.getValue();
            boolean gotLock = false;
            try {
               gotLock = wrapper.tryLock();
               if (gotLock) {

                  // since we got here we're done with this instance,
                  // so add it's key to the list of keys we plan don't
                  // need to return to.
                  keysToRemove.add(key);

                  Object instance = wrapper.getInstance();
                  try {
                     if (prototype.invokeEvictable(instance)) {
                        wrapper.markEvicted();
                        prototype.passivate(wrapper.getInstance());
                        wrapper.markPassivated();
                        instances.remove(key);
                     }
                  } 
                  catch (InvocationTargetException e)
                  {
                     logger.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) + 
                           " resulted in an exception.",e.getCause());
                  }
                  catch (IllegalAccessException e)
                  {
                     logger.warn("It appears that the method for checking the eviction or passivating the Mp " + SafeString.objectDescription(instance) + 
                           " is not defined correctly. Is it visible?",e);
                  }
                  catch (RuntimeException e)
                  {
                     logger.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) + 
                           " resulted in an exception.",e);
                  }
               }
            } finally {
               if (gotLock)
                  wrapper.releaseLock();
            }

         }

         // now clean up everything we managed to get hold of
         for (Object key : keysToRemove)
            instancesToEvict.remove(key);

      }

      statCollector.evictionPassCompleted();
   }

   public void startEvictionThread(long evictionFrequency, TimeUnit timeUnit) {
      if (0 == evictionFrequency || null == timeUnit) {
         logger.warn("Eviction Thread cannot start with zero frequency or null TimeUnit {} {}", evictionFrequency, timeUnit);
         return;
      }

      if (prototype != null && prototype.isEvictableSupported()) {
         evictionScheduler = Executors.newSingleThreadScheduledExecutor();
         evictionScheduler.scheduleWithFixedDelay(new Runnable(){ public void run(){ evict(); }}, evictionFrequency, evictionFrequency, timeUnit);
      }
   }

   private ExecutorService outputExecutorService = null;
   private int outputConcurrency = -1;
   private Object lockForExecutorServiceSetter = new Object();

   @Override
   public void setConcurrency(int concurrency)
   {
      synchronized(lockForExecutorServiceSetter)
      {
         outputConcurrency = concurrency;
         if (prototype != null) // otherwise this isn't initialized yet
            setupOutputConcurrency();
      }
   }
   
   private void setupOutputConcurrency()
   {
      if (prototype.isOutputSupported() && isRunning)
      {
         synchronized(lockForExecutorServiceSetter)
         {
            if (outputConcurrency > 1)
               outputExecutorService = Executors.newFixedThreadPool(outputConcurrency);
            else
            {
               if (outputExecutorService != null)
                  outputExecutorService.shutdown();
               outputExecutorService = null;
            }
         }
      }
   }

   // This method MUST NOT THROW
   public void outputPass() {
      if (!prototype.isOutputSupported())
         return;

      // take a snapshot of the current container state.
      LinkedList<InstanceWrapper> toOutput = new LinkedList<InstanceWrapper>(instances.values());

      Executor executorService = null;
      Semaphore taskLock = null;
      synchronized(lockForExecutorServiceSetter)
      {
         executorService = outputExecutorService;
         if (executorService != null)
            taskLock = new Semaphore(outputConcurrency);
      }

      // This keeps track of the number of concurrently running
      // output tasks so that this method can wait until they're
      // all done to return.
      //
      // It's also used as a condition variable signaling on its
      // own state changes.
      final AtomicLong numExecutingOutputs = new AtomicLong(0);

      // keep going until all of the outputs have been invoked
      while (toOutput.size() > 0 && isRunning)
      {
         for (final Iterator<InstanceWrapper> iter = toOutput.iterator(); iter.hasNext();) 
         {
            final InstanceWrapper wrapper = iter.next();
            boolean gotLock = false;

            gotLock = wrapper.tryLock();

            if (gotLock) 
            {
               // If we've been evicted then we're on our way out
               // so don't do anything else with this.
               if (wrapper.isEvicted())
               {
                  iter.remove();
                  wrapper.releaseLock();
                  continue;
               } 

               final Object instance = wrapper.getInstance(); // only called while holding the lock
               final Semaphore taskSepaphore = taskLock;

               // This task will release the wrapper's lock.
               Runnable task = new Runnable()
               {
                  @Override
                  public void run()
                  {
                     try
                     {
                        if (isRunning && !wrapper.isEvicted()) 
                           invokeOperation(instance, Operation.output, null); 
                     }
                     finally 
                     {
                        wrapper.releaseLock();

                        // this signals that we're done.
                        synchronized(numExecutingOutputs)
                        {
                           numExecutingOutputs.decrementAndGet();
                           numExecutingOutputs.notifyAll();
                        }
                        if (taskSepaphore != null) taskSepaphore.release(); 
                     }
                  }
               };

               synchronized(numExecutingOutputs)
               {
                  numExecutingOutputs.incrementAndGet();
               }

               if (executorService != null)
               {
                  try
                  {
                     taskSepaphore.acquire();
                     executorService.execute(task);
                  }
                  catch (RejectedExecutionException e)
                  {
                     // this may happen because of a race condition between the 
                     taskSepaphore.release();
                  }
                  catch (InterruptedException e)
                  {
                     // this can happen while blocked in the semaphore.acquire.
                     // if we're no longer running we should just get out
                     // of here.
                  }
               }
               else
                  task.run();

               iter.remove();
            } // end if we got the lock
         } // end loop over every Mp
      } // end while there are still Mps that haven't had output invoked.

      // =======================================================
      // now make sure all of the running tasks have completed
      synchronized(numExecutingOutputs)
      {
         while (numExecutingOutputs.get() > 0)
         {
            try { numExecutingOutputs.wait(); }
            catch (InterruptedException e)
            {
               // if we were interupted for a shutdown then just stop
               // waiting for all of the threads to finish
               if (!isRunning)
                  break;
               // otherwise continue checking.
            }
         }
      }
      // =======================================================
   }

   @Override
   public void invokeOutput() {
      statCollector.outputInvokeStarted();
      outputPass();
      statCollector.outputInvokeCompleted();
   }

   //----------------------------------------------------------------------------
   // Internals
   //----------------------------------------------------------------------------

   private Object getKeyFromMessage(Object message) throws ContainerException
   {
      Object key = null;
      try
      {
         key = keyMethods.invokeGetter(message);
      }
      catch (IllegalArgumentException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               ". Are you sure that the method to retrieve the key takes no parameters?",e);
      }
      catch(IllegalAccessException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               ". Are you sure that the method to retrieve the key is publically accessible (both the class and the method must be public)?");
      }
      catch(InvocationTargetException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               " because the method to retrieve the key threw an exception.",e.getCause());
      }

      if (key == null)
         throw new ContainerException("the container for " + clusterId + " retrieved a null message key from " + 
               SafeString.objectDescription(message));
      return key;
   }

   /**
    * This is required to return non null or throw a ContainerException
    * @throws IllegalAccessException 
    * @throws InvocationTargetException 
    */
   public InstanceWrapper getInstanceForKey(Object key) throws ContainerException
   {
      // common case has "no" contention
      InstanceWrapper wrapper = instances.get(key);
      if(wrapper != null)
         return wrapper;

      // otherwise we'll do an atomic check-and-update
      synchronized (this)
      {
         wrapper = instances.get(key); // double checked lock?????
         if (wrapper != null)
            return wrapper;

         Object instance = null;
         try
         {
            instance = prototype.newInstance();
         }
         catch(InvocationTargetException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone method threw an exception.",e.getCause());
         }
         catch(IllegalAccessException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone method is not accessible. Is the class public? Is the clone method public? Does the class implement Cloneable?",e);
         }
         catch(RuntimeException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone invocation resulted in an unknown exception.",e);
         }

         if (instance == null)
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                  ". The value returned from the clone call appears to be null.");

         wrapper = new InstanceWrapper(instance); // null check above.
         instances.put(key, wrapper);
         statCollector.messageProcessorCreated(key);

         // activate
         byte[] data = null;
         if (logger.isTraceEnabled())
            logger.trace("the container for " + clusterId + " is activating instance " + String.valueOf(instance)
                  + " with " + ((data != null) ? data.length : 0) + " bytes of data"
                  + " via " + SafeString.valueOf(prototype));

         try
         {
            prototype.activate(instance, key, data);
         }
         catch(IllegalArgumentException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  ". Is it declared to take a byte[]?",e);
         }
         catch(IllegalAccessException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  ". Is the active method accessible - the class is public and the method is public?",e);
         }
         catch(InvocationTargetException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  " because the method itself threw an exception.",e.getCause());
         }
         catch(RuntimeException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  " because of an unknown exception.",e);
         }

         return wrapper;
      }
   }

   public enum Operation { handle, output };

   /**
    * helper method to invoke an operation (handle a message or run output) handling all of hte
    * exceptions and forwarding any results.
    */
   private void invokeOperation(Object instance, Operation op, Object message)
   {
      if (instance != null) // possibly passivated ...
      {
         try
         {
            statCollector.messageDispatched(message);
            Object result = op == Operation.output ? prototype.invokeOutput(instance) : prototype.invoke(instance, message);
            statCollector.messageProcessed(message);
            if (result != null)
            {
               dispatcher.dispatch(result);
            }
         }
         catch(ContainerException e)
         {
            logger.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " + 
                  SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + SafeString.objectDescription(message)) : ""),e);
            statCollector.messageFailed();
         }
         // this is an exception thrown as a result of the reflected call having an illegal argument.
         // This should actually be impossible since the container itself manages the calling.
         catch(IllegalArgumentException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?", e);
            statCollector.messageFailed();
         }
         // can't access the method? Did the app developer annotate it correctly?
         catch(IllegalAccessException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due an access problem. Is the method public?", e);
            statCollector.messageFailed();
         }
         // The app threw an exception.
         catch(InvocationTargetException e)
         {
            logger.warn("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " because an exception was thrown by the Message Processeor itself.", e.getCause() );
            statCollector.messageFailed();
         }
         // RuntimeExceptions bookeeping
         catch (RuntimeException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due to an unknown exception.", e);
            statCollector.messageFailed();

            if (op == Operation.handle)
               throw e;
         }
      }
   }

}
