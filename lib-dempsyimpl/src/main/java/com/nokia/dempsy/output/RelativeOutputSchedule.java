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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
 

/**
 * This is a default implementation of OutputExecuter. At fixed time interval,
 * this class will invoke the @Output method of MPs
 * 
 */
public class RelativeOutputSchedule implements OutputExecuter {
  private long interval;
  private TimeUnit timeUnit;
  private OutputInvoker outputInvoker;
  private ScheduledExecutorService scheduler;

  public RelativeOutputSchedule(long interval, TimeUnit timeUnit) {
    this.interval = interval;
    this.timeUnit = timeUnit;
  }

  @Override
  public void setOutputInvoker(OutputInvoker outputInvoker) {
    this.outputInvoker = outputInvoker;

  }
 
/**
 * Container will invoke this method
 */
  @Override
  public void start() {
    scheduler= Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleWithFixedDelay(new RunOutput(), 0, interval, timeUnit);
  }

  private class RunOutput implements Runnable {

    private RunOutput() {
      run();
    }

    @Override
    public void run() {
      //calling container -outputpass method to execute @Output method
      outputInvoker.invokeOutput();
    }
  }

  /**
   * Container will invoke this method
   */
  @Override
  public void stop() {
    //gracefully shutting down
    scheduler.shutdown();
  }
}
