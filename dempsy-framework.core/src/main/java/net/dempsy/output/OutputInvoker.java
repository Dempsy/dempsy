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

import net.dempsy.lifecycle.annotation.Output;

/**
 * This is a server side interface to invoke the @Output methods of MPs.
 * Dempsy will provide an implementation of this interface. During startup, 
 * Dempsy will set an instance of OutputInovker to the client output scheduler.
 * Using this instance, client application will call @Output method of MPs.
 */
public interface OutputInvoker {

    /**
     * <p>This method will be called by client application to invoke the @Output methods of MPs.
     * Dempsy will execute the @Output method of all MPs. During the Output execution, MP will not
     * receive any incoming message.</p>
     * 
     */
    public void invokeOutput();

    /**
     * If the {@link OutputScheduler} wants the container to run the {@link Output} pass in 
     * a multithreaded manner, then calling this method and setting the <code>concurrency</code>
     * to greater than <code>1</code> will do that. This method will usually be called
     * once, at initialization. Each time it's called the {@link Executor} used by the 
     * container will be recreated with all of its attending threads so this method
     * should <b>not</b> be called with each invokeOutput call.
     *  
     * @param concurrency is the number of threads to allow {@link Output} to be run 
     * simultaneously.
     */
    public void setOutputConcurrency(int concurrency);

}
