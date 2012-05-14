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
 * This is a server side interface to invoke the @Output methods of MPs.
 * Dempsy will provide an implementation of this interface. During startup, Dempsy will set an instance of OutputInovker to
 * the client output scheduler. Using this instance, client application will  call @Output method of MPs.
 * 
 */

public interface OutputInvoker {

  /**
   * This method will be called by client application to invoke the @Output methods of MPs.
   * Dempsy will execute the @Output method of all MPs. During the Output execution, MP will not receive any incoming message. 
   */
  public void invokeOutput();
}

