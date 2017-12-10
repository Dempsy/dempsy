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

package net.dempsy.messages;

/**
 * An {@link Adaptor} is used to adapt data streams from external sources
 * into Dempsy. Most Dempsy applications contain at least one adaptor. The
 * Adaptor needs to simply acquire the data meant to be processed by Dempsy 
 * and use the {@link Dispatcher} to send the message onward.
 */
public interface Adaptor extends AutoCloseable {
    /**
    * This will be called by Dempsy to provide the {@link Dispatcher} to the
    * Adaptor. Any object that is the Dispatched will be sent to message
    * processors.
    */
    public void setDispatcher(Dispatcher dispatcher);

    /**
    * start will be called by Dempsy to tell the Adaptor that it can begin
    * dispatching messages with the {@link Dispatcher}. start() will always be
    * called after Adpator.setDispatcher. This method is not expected to return
    * until {@link #stop()} is called from another thread.
    */
    public void start();

    /**
    * This will be called by Dempsy in order to get the Adaptor to stop. Under normal circumstances
    * the "start()" is still executing and the "stop()" should cause the "start()" to return. If
    * it doesn't then Dempsy will most likely hang.
    */
    public void stop();

    @Override
    public default void close() {
        stop();
    }

}
