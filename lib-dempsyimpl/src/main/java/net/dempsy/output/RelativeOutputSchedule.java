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

import java.util.concurrent.TimeUnit;

import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
 
/**
 * This is a default implementation of OutputExecuter. At fixed time interval,
 * this class will invoke the @Output method of MPs
 */
public class RelativeOutputSchedule extends AbstractOutputSchedule {
  
  /** The interval. */
  private long interval;
  
  /** The time unit. */
  private TimeUnit timeUnit;
  
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

/**
 * Container will invoke this method.
 */
  @Override
  public void start() {
    try {
      JobDetail jobDetail = super.getJobDetail();
      Trigger trigger = getSimpleTrigger(timeUnit, (int) interval);
      scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.scheduleJob(jobDetail, trigger);
      scheduler.start();
    } catch (SchedulerException se) { 
      logger.error("Error occurred while starting the relative scheduler : " + se.getMessage(), se);
    }
  }

  /**
   * Gets the simple trigger.
   *
   * @param timeUnit the time unit
   * @param timeInterval the time interval
   * @return the simple trigger
   */
  private Trigger getSimpleTrigger(TimeUnit timeUnit, int timeInterval) {
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
      throw new RuntimeException("The " + RelativeOutputSchedule.class.getSimpleName() + " cannot handle a time unit of " + timeUnit); //default 1 sec
    }
    Trigger simpleTrigger = TriggerBuilder.newTrigger().withSchedule(simpleScheduleBuilder).build();
    return simpleTrigger;
  }

}
