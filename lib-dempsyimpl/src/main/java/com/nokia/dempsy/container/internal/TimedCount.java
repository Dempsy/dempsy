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

package com.nokia.dempsy.container.internal;

import java.util.concurrent.atomic.AtomicLong;


/**
 *  A single counter that keeps track of the first and last times that the
 *  {@link #increment} or {@link #add} were called. Useful to keep track of
 *  a series of events, where the reporting points span a larger time than
 *  the event times (so if you simply calculated start/end based on reporting
 *  points, you'd report an incorrectly low event rate).
 *  <p>
 *  <em>Warning:</em> concurrent updates and reads will lead to a race
 *  condition in which new events may arrive between reading the count and
 *  elapsed time. In practice, you shouldn't care.
 */
public class TimedCount
{
   AtomicLong firstEventTime = new AtomicLong();   // will be using 0 as flag
   AtomicLong lastEventTime = new AtomicLong();
   AtomicLong value = new AtomicLong();

   AtomicLong curMinuteValue = new AtomicLong();
   AtomicLong lastMinuteValue = new AtomicLong();


//----------------------------------------------------------------------------
//  Public Methods
//----------------------------------------------------------------------------

   /**
    *  Increments the counter, using the current time as a timestamp.
    */
   public void increment()
   {
      increment(System.currentTimeMillis());
   }


   /**
    *  Increments the counter, using the specified timestamp. This method is
    *  exposed for testing.
    */
   public void increment(long timestamp)
   {
      firstEventTime.compareAndSet(0, timestamp);
      lastEventTime.set(timestamp);
      value.incrementAndGet();
   }


   /**
    *  Returns the number of events that have been received since the start.
    */
   public long getValue()
   {
      return value.get();
   }


   /**
    *  Returns the elapsed time between the first and last event. Will be 0
    *  in the following case: (1) there haven't been any events, (2) there has
    *  been a single event, (3) all events arrived at the same time (unlikely).
    *  <p>
    *  May return a spurious value if {@link #reset} was called in the middle
    *  of an update.
    */
   public long getElapsedMillis()
   {
      return getLastEventTimestamp() - getFirstEventTimestamp();
   }


   /**
    *  Returns the Java timestamp of the first event, 0 if there has not been
    *  an event. For most purposes, {@link #getElapsedTime} is a better choice.
    */
   public long getFirstEventTimestamp()
   {
      Long fet = firstEventTime.get();
      return (fet == null) ? 0 : fet.longValue();
   }


   /**
    *  Returns the Java timestamp of the last event, 0 if there has not been
    *  an event. For most purposes, {@link #getElapsedTime} is a better choice.
    */
   public long getLastEventTimestamp()
   {
      return lastEventTime.get();
   }


   /**
    *  Returns the rate that events have arrived. Returns 0 if no events have
    *  been logged. If only a single event has been received, assumes one
    *  second of elapsed time (because the actual elapsed time is 0, meaning
    *  that the rate is infinity).
    */
   public double getRatePerSecond()
   {
      long elapsed = getElapsedMillis();
      return (elapsed == 0) ? getValue() : getValue() * 1000.0 / elapsed;
   }


   /**
    *  Resets the counter and receipt times.
    *  <p>
    *  <em>Warning:</em> this method is not thread-safe: one thread could attempt
    *  to reset while another is in the middle of an update. In the expected use
    *  case (a JMX page) this should not be an issue: there might be a few events
    *  that don't get counted.
    */
   public void reset()
   {
      // by resetting the first event time last, we can use it as a check for
      // the retrieval methods (ie, if it's not null, we've received *some*
      // events) -- but we might overcount
      value.set(0L);
      lastEventTime.set(0L);
      firstEventTime.set(0L);
   }
}
