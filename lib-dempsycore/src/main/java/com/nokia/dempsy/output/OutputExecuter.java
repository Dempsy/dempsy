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

/**
 * An {@link OutputExecuter} is used to allow external application[s] to invoke
 * the @output methods of MPs. Client (External) Application can implement their
 * custom scheduler to invoke the @Output method of MPs. Dempsy framework would
 * inject the OutputInvoker instance during startup, and invoke the start
 * method.
 * 
 */
public interface OutputExecuter {
  /**
   * This method will be called by Dempsy framework to provide the
   * {@link OutputInvoker} to the Output.
   */
  public void setOutputInvoker(OutputInvoker outputInvoker);

  /**
   * This method is called by the MpContainer
   */
  public void start();

  /**
   * This method is called by the MpContainer
   */
  public void stop();
}
