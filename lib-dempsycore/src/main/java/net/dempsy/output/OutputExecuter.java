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

/**
 * This interface enables the client application to implement their own custom
 * output scheduler and plug-in to Dempsy. Using this scheduler, client
 * application will take responsibility to invoke the @output methods of MPs.
 * During startup, Dempsy will provide an instance of OutputInvoker to the
 * scheduler, and executes the start method to trigger the output schedule.
 * 
 */

public interface OutputExecuter {

  /**
   * This method will be called by Dempsy to provide the instance of
   * outputInvoker.
   * 
   * @param outputInvoker   the output invoker
   * 
   */
  public void setOutputInvoker(OutputInvoker outputInvoker);

  /**
   * This method will be called by Dempsy to tell the OutputExecuter that it can
   * begin executing the @Output methods of MPs with the {@link OutputInvoker}.
   * {@link #start()} will always be called after {@link #setOutputInvoker()} .
   * This method execution will be stopped, when {@link #stop()} is called from
   * Dempsy.
   */

  public void start();

  /**
   * This method will be called by Dempsy in order to get the OutputExecuter to
   * stop executing @Output methods of MPs. Under normal circumstances the
   * {@link #start()} is still executing and the {@link #stop()} should cause
   * the {@link #start()} to stop.
   */
  public void stop();
}
