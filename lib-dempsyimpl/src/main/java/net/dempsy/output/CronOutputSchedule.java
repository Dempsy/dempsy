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

package net.dempsy.output;

import java.text.ParseException;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import net.dempsy.output.OutputExecuter;


/**
 * The Class CronOutputScheduler. 
 * This class executes @Output method on MPs based on provided cron time expression (example: '*\/1 * * * * ?', run job at every one sec) 
 *  
 */
public class CronOutputSchedule extends AbstractOutputSchedule implements OutputExecuter {

  /** The cron expression. */
  private String cronExpression;
  
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
      JobDetail jobDetail = super.getJobDetail();
      Trigger trigger = getCronTrigger(cronExpression);
      scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.scheduleJob(jobDetail, trigger);
      scheduler.start();
    } catch (SchedulerException se) {
      logger.error("Error occurred while starting the cron scheduler : " + se.getMessage(), se);
    }
  }

  /**
   * Gets the cron trigger.
   *
   * @param cronExpression the cron expression
   * @return the cron trigger
   */
  private Trigger getCronTrigger(String cronExpression) {
    CronScheduleBuilder cronScheduleBuilder = null;
    Trigger cronTrigger = null;
    try {
      cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
      cronScheduleBuilder.withMisfireHandlingInstructionFireAndProceed();
      TriggerBuilder<Trigger> cronTtriggerBuilder = TriggerBuilder.newTrigger();
      cronTtriggerBuilder.withSchedule(cronScheduleBuilder);
      cronTrigger = cronTtriggerBuilder.build();
    } catch (ParseException pe) {
      logger.error("Error occurred while builiding the cronTrigger : " + pe.getMessage(), pe);
    }
    return cronTrigger;
  }


}
