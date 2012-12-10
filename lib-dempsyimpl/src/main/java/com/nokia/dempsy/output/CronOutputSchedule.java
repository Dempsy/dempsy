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

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class CronOutputScheduler. 
 * This class executes @Output method on MPs based on provided cron time expression (example: '*\/1 * * * * ?', run job at every one sec) 
 *  
 */
public class CronOutputSchedule implements OutputExecuter {

  /** The logger. */
  private static Logger logger = LoggerFactory.getLogger(CronOutputSchedule.class);
  
  /** The scheduler. */
  private Scheduler scheduler;
  
  /** The cron expression. */
  private String cronExpression;
  
  /** The output invoker. */
  private OutputInvoker outputInvoker;
  
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
   * Instantiates a new cron output scheduler.
   *
   * @param cronExpression the cron expression
   */
  public CronOutputSchedule(String cronExpression) {
    this.cronExpression = cronExpression;
  }

   /* (non-Javadoc)
   * @see com.nokia.dempsy.output.OutputExecuter#start()
   */
  public void start() {
    try {
      OutputQuartzHelper outputQuartzHelper = new OutputQuartzHelper();
      JobDetail jobDetail = outputQuartzHelper.getJobDetail(outputInvoker);
      Trigger trigger = outputQuartzHelper.getCronTrigger(cronExpression);
      scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.scheduleJob(jobDetail, trigger);
      scheduler.start();
    } catch (SchedulerException se) {
      logger.error("Error occurred while starting the cron scheduler : " + se.getMessage(), se);
    }
  }

  /* (non-Javadoc)
   * @see com.nokia.dempsy.output.OutputExecuter#stop()
   */
  public void stop() {
    try {
      // gracefully shutting down
      scheduler.shutdown(true);
    } catch (SchedulerException se) {
      logger.error("Error occurred while stopping the cron scheduler : " + se.getMessage(), se);
    }
  }

  /* (non-Javadoc)
   * @see com.nokia.dempsy.output.OutputExecuter#setOutputInvoker(com.nokia.dempsy.output.OutputInvoker)
   */
  public void setOutputInvoker(OutputInvoker outputInvoker) {
    this.outputInvoker = outputInvoker;
    
    if (concurrency > 1)
       outputInvoker.setConcurrency(concurrency);
  }

}
