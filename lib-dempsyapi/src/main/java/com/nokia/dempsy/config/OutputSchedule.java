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

package com.nokia.dempsy.config;

import java.util.concurrent.TimeUnit;

import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;

/**
 * This class can be configured within a {@link ClusterDefinition} in order to setup
 * a periodic call of any {@link MessageProcessor}'s methods that are annotated with
 * the {@link Output} annotation. 
 */
public final class OutputSchedule
{
   private long interval;
   private TimeUnit timeUnit;
   
   /**
    * Constuct an OutputSchedule from an inteval and timeUnit.
    */
   public OutputSchedule(long interval, TimeUnit timeUnit)
   {
      this.interval = interval;
      this.timeUnit = timeUnit;
   }

   /**
    * Convenience mechanism for Dependency injection containers that work from
    * configuration files (e.g. Spring). The timeUnits parameter must be a valid
    * String value that can be passed to {@link TimeUnit}.valueOf(). 
    */
   public OutputSchedule(long interval, String timeUnit)
   {
      this(interval,TimeUnit.valueOf(timeUnit));
   }

   public Long getInterval()
   {
      return interval;
   }

   public TimeUnit getTimeUnit()
   {
      return timeUnit;
   }
   
}
