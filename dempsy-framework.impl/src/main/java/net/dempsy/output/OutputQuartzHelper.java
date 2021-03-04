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

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputQuartzHelper.class);

    /** The OUTPUT JOB NAME. */
    public static final String OUTPUT_JOB_NAME = "outputInvoker";

    /**
     * Gets the job detail.
     *
     * @param outputInvoker the output invoker
     * @return the job detail
     */
    public JobDetail getJobDetail(final OutputInvoker outputInvoker) {
        final JobBuilder jobBuilder = JobBuilder.newJob(OutputJob.class);
        final JobDetail jobDetail = jobBuilder.build();
        jobDetail.getJobDataMap().put(OUTPUT_JOB_NAME, outputInvoker);
        return jobDetail;
    }

    /**
     * Gets the cron trigger.
     *
     * @param cronExpression the cron expression
     * @return the cron trigger
     */
    public Trigger getCronTrigger(final String cronExpression) {
        CronScheduleBuilder cronScheduleBuilder = null;
        Trigger cronTrigger = null;
        try {
            cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
            cronScheduleBuilder.withMisfireHandlingInstructionFireAndProceed();
            final TriggerBuilder<Trigger> cronTtriggerBuilder = TriggerBuilder.newTrigger();
            cronTtriggerBuilder.withSchedule(cronScheduleBuilder);
            cronTrigger = cronTtriggerBuilder.build();
        } catch(final Exception pe) {
            LOGGER.error("Error occurred while builiding the cronTrigger : " + pe.getMessage(), pe);
        }
        return cronTrigger;
    }

}
