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

package com.nokia.dempsy.monitoring;

public interface StatsCollector {

   /**
    * A timer context is returned from start calls on the Stats collector
    * to provide a thread-safe context for stopping the started timer. This
    * is analagous to Yammer Metrics use of Times. 
    */
   public static interface TimerContext
   {
      public void stop();
   }

   /**
    *  The dispatcher calls this method in its <code>onMessage</code> handler.
    */
   void messageReceived(byte[] message);

	/**
	 * MPContainer calls this method when before invoking an MP's 
	 * <code>MessageHandler</code> or Output method.
	 */
	void messageDispatched(Object message);

	/**
	 * MPContainer calls this method when successfully invoking an MP's
	 * <code>MessageHandler</code> or Output method.
	 */
	void messageProcessed(Object message);	

	
	/**
	 * MPContainer calls this method when invoking an MP's
	 * <code>MessageHandler</code> or Output method results in an error.
	 */
	void messageFailed(boolean mpFailure);
	
	/**
	 * Dispatcher calls this method when emitting a message
	 */
	void messageSent(byte[] message);
	
	/**
	 * Dispatcher calls this method when it fails to dispatch
	 * a message
	 */
	void messageNotSent(Object message);
	
	/**
	 *  The dispatcher calls this method in its <code>onMessage</code> handler
	 *  when it discards a message.
	 */
	void messageDiscarded(Object message);
	
	/**
	 *  The MP manager calls this method when it creates a message processor
	 *  instance.
	 */
	void messageProcessorCreated(Object key);
	
	/**
	 *  The instance manager calls this method when it deletes a message processor
	 *  instance.
	 */
	void messageProcessorDeleted(Object key);

	/**
	 * Some stats collectors need to be stopped.
	 */
   public void stop();
   
   /**
    * Dempsy calls into this just before starting pre-instantiation.
    */
   public TimerContext preInstantiationStarted();
   
   /**
    * Dempsy calls into this just before invoking an MPs message handler.
    */
   public TimerContext handleMessageStarted();
   
   /**
    * Dempsy calls into this just before calling @Output methods for MPs.
    */
   public TimerContext outputInvokeStarted();
   
   /**
    * Dempsy calls into this just before calling @Output methods for MPs.
    */
   public TimerContext evictionPassStarted();
   
	// FIXME
	/*
	 *------------------------------------------------------------------------- 
	 * Methods for testing only.
	 * Serveral Unit tests read stats, which in the prior MBean implementation
	 * was a free ride.  We do need to think about how this can be done better.
	 * Not all implementations would really need to implement these, just one
	 * for testing.  But for the moment, we'll leave it here and avoid casting
	 * all over in test cases.
	 *-------------------------------------------------------------------------
	 */
	
	long getProcessedMessageCount();
	long getDispatchedMessageCount();
	long getMessageFailedCount();
	long getDiscardedMessageCount();
	int getInFlightMessageCount();
	double getPreInstantiationDuration();
	double getOutputInvokeDuration();
	double getEvictionDuration();
	
}
