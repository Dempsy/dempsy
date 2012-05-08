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
 *  <p>This is a Server side interface for the Output abstraction. 
 *  It enables the client application[s] to invoke the @Output method call for all MPs</p>
 *  It's implementor class would get hold of container and call @Output method for all MPs
 */

public interface OutputInvoker
{
	/**
	 *  This method is invoked by client application to execute @Output method on all MPs.
	 *   
	 */
	  public void invokeOutput();
}
