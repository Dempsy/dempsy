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

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class OutputQuartzHelper.
 * This is a quartz helper class,it contains methods to provide Quartz JobDetail, Simple Quartz Trigger, and Cron Trigger objects.  
 */
public class OutputQuartzHelper {
  
  /** The logger. */
  private static Logger logger = LoggerFactory.getLogger(OutputQuartzHelper.class);
  
  /** The OUTPUT JOB NAME. */
  public static String OUTPUT_JOB_NAME = "outputInvoker";

  /**
   * Gets the job detail.
   *
   * @param outputInvoker the output invoker
   * @return the job detail
   */
  public JobDetail getJobDetail(OutputInvoker outputInvoker) {
    JobBuilder jobBuilder = JobBuilder.newJob(OutputJob.class);
    JobDetail jobDetail = jobBuilder.build();
    jobDetail.getJobDataMap().put(OUTPUT_JOB_NAME, outputInvoker);
    return jobDetail;
  }

  /**
   * Gets the simple trigger.
   *
   * @param timeUnit the time unit
   * @param timeInterval the time interval
   * @return the simple trigger
   */
  public Trigger getSimpleTrigger(TimeUnit timeUnit, int timeInterval) {
    SimpleScheduleBuilder simpleScheduleBuilder = null;
    simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule();

    switch (timeUnit) {
    case MILLISECONDS:
      simpleScheduleBuilder.withIntervalInMilliseconds(timeInterval).repeatForever();
      break;
    case SECONDS:
      simpleScheduleBuilder.withIntervalInSeconds(timeInterval).repeatForever();
      break;
    case MINUTES:
      simpleScheduleBuilder.withIntervalInMinutes(timeInterval).repeatForever();
      break;
    case HOURS:
      simpleScheduleBuilder.withIntervalInHours(timeInterval).repeatForever();
      break;
    case DAYS:
      simpleScheduleBuilder.withIntervalInHours(timeInterval * 24).repeatForever();
      break;
    default:
      simpleScheduleBuilder.withIntervalInSeconds(1).repeatForever(); //default 1 sec
    }
    Trigger simpleTrigger = TriggerBuilder.newTrigger().withSchedule(simpleScheduleBuilder).build();
    return simpleTrigger;
  }

  /**
   * Gets the cron trigger.
   *
   * @param cronExpression the cron expression
   * @return the cron trigger
   */
  public Trigger getCronTrigger(String cronExpression) {
    CronScheduleBuilder cronScheduleBuilder = null;
    Trigger cronTrigger = null;
    try {
      cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
      cronScheduleBuilder.withMisfireHandlingInstructionFireAndProceed();
      TriggerBuilder<Trigger> cronTtriggerBuilder = TriggerBuilder.newTrigger();
      cronTtriggerBuilder.withSchedule(cronScheduleBuilder);
      cronTrigger = cronTtriggerBuilder.build();
    } catch (Exception pe) {
      logger.error("Error occurred while builiding the cronTrigger : " + pe.getMessage(), pe);
    }
    return cronTrigger;
  }

}
