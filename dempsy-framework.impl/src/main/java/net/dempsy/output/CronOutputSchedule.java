/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.output;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;

/**
 * The Class CronOutputScheduler.
 * This class executes @Output method on MPs based on provided cron time expression (example: '*\/1 * * * * ?', run job at every one sec)
 *
 */
public class CronOutputSchedule implements OutputScheduler {

    /** The logger. */
    private static Logger LOGGER = LoggerFactory.getLogger(CronOutputSchedule.class);

    /** The scheduler. */
    private Scheduler scheduler;

    /** The cron expression. */
    private final String cronExpression;

    /** The output invoker. */
    private OutputInvoker outputInvoker;

    /**
     * Instantiates a new cron output scheduler.
     *
     * @param cronExpression the cron expression
     */
    public CronOutputSchedule(final String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public void start(final Infrastructure infra) {
        LOGGER.info("Starting cron output scheduler for " + outputInvoker);
        // There seems to be a bug in Quartz where getting the default scheduler causes a failure
        // when done in parallel.
        synchronized(StdSchedulerFactory.class) {
            try {
                final OutputQuartzHelper outputQuartzHelper = new OutputQuartzHelper();
                final JobDetail jobDetail = outputQuartzHelper.getJobDetail(outputInvoker);
                final Trigger trigger = outputQuartzHelper.getCronTrigger(cronExpression);
                scheduler = StdSchedulerFactory.getDefaultScheduler();
                scheduler.scheduleJob(jobDetail, trigger);
                scheduler.start();
                LOGGER.info("Started cron output scheduler for " + outputInvoker);
            } catch(final SchedulerException se) {
                LOGGER.error("Error occurred while starting the cron scheduler : " + se.getMessage(), se);
            }
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void stop() {
        try {
            // gracefully shutting down
            scheduler.shutdown(false);
        } catch(final SchedulerException se) {
            LOGGER.error("Error occurred while stopping the cron scheduler : " + se.getMessage(), se);
        }
    }

    @Override
    public void setOutputInvoker(final OutputInvoker outputInvoker) {
        if(this.outputInvoker != null) {
            LOGGER.error("Cannot supply a second output invoker to a " + CronOutputSchedule.class.getSimpleName()
                + ". Do you have the same instance of the " + CronOutputSchedule.class.getSimpleName() + " being used in more than one container?");
            throw new IllegalStateException("Cannot supply a second output invoker to a " + CronOutputSchedule.class.getSimpleName()
                + ". Do you have the same instance of the " + CronOutputSchedule.class.getSimpleName() + " being used in more than one container?");
        }
        this.outputInvoker = outputInvoker;
    }

}
