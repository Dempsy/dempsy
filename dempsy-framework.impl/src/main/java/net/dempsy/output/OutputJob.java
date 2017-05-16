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

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class OutputJob. This class is responsible to call invokeOutput() method.
 * This class is being called by Quartz scheduler.
 */
public class OutputJob implements Job {

  /** The logger. */
  private static Logger logger = LoggerFactory.getLogger(OutputJob.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
   */
  public void execute(JobExecutionContext context) throws JobExecutionException {

    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    OutputInvoker outputInvoker = (OutputInvoker)dataMap.get(OutputQuartzHelper.OUTPUT_JOB_NAME);

    if (outputInvoker != null) {
      // execute MP's output method
      outputInvoker.invokeOutput();
    } else {
      logger.warn("outputInvoker is NULL");
    }
  }

}
