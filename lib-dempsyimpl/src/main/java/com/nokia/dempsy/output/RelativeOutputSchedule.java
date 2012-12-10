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

package com.nokia.dempsy.output;

import java.util.concurrent.TimeUnit;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * This is a default implementation of OutputExecuter. At fixed time interval,
 * this class will invoke the @Output method of MPs
 */
public class RelativeOutputSchedule implements OutputExecuter {
  
  /** The logger. */
  private static Logger logger = LoggerFactory.getLogger(RelativeOutputSchedule.class);
  
  /** The interval. */
  private long interval;
  
  /** The time unit. */
  private TimeUnit timeUnit;
  
  /** The output invoker. */
  private OutputInvoker outputInvoker;
  
  /** The scheduler. */
  private Scheduler scheduler;
  
  /** Contains the number of threads to set on the {@link OutputInvoker} */
  private int concurrency = -1;

  public int getConcurrency()
  {
     return concurrency;
  }

  public void setConcurrency(int concurrency)
  {
     this.concurrency = concurrency;
  }

  /**
   * Instantiates a new relative output schedule.
   *
   * @param interval the interval
   * @param timeUnit the time unit
   */
  public RelativeOutputSchedule(long interval, TimeUnit timeUnit) {
    this.interval = interval;
    this.timeUnit = timeUnit;
  }

  /* (non-Javadoc)
   * @see com.nokia.dempsy.output.OutputExecuter#setOutputInvoker(com.nokia.dempsy.output.OutputInvoker)
   */
  @Override
  public void setOutputInvoker(OutputInvoker outputInvoker) {
    this.outputInvoker = outputInvoker;

    if (concurrency > 1)
       outputInvoker.setConcurrency(concurrency);
  }
 
/**
 * Container will invoke this method.
 */
  @Override
  public void start() {
    try {
      OutputQuartzHelper outputQuartzHelper = new OutputQuartzHelper();
      JobDetail jobDetail = outputQuartzHelper.getJobDetail(outputInvoker);
      Trigger trigger = outputQuartzHelper.getSimpleTrigger(timeUnit, (int) interval);
      scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.scheduleJob(jobDetail, trigger);
      scheduler.start();
    } catch (SchedulerException se) { 
      logger.error("Error occurred while starting the relative scheduler : " + se.getMessage(), se);
    }
  }
 

  /**
   * Container will invoke this method.
   */
  @Override
  public void stop() {
    try {
      // gracefully shutting down
      scheduler.shutdown(true);
    } catch (SchedulerException se) {
      logger.error("Error occurred while stopping the relative scheduler : " + se.getMessage(), se);
    }
  }
}
