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

package com.nokia.dempsy.messagetransport;

import com.nokia.dempsy.monitoring.StatsCollector;

public interface Receiver
{
   /**
    * Provide the Destination for this reciever.
    */
   public Destination getDestination() throws MessageTransportException;
   
   /**
    * Provide the message consumer to this receiver.
    */
   public void setListener(Listener listener) throws MessageTransportException;
   
   /**
    * Dempsy will call this on start. Throwing an exception will cause Dempsy
    * to fail to start.
    */
   public void start() throws MessageTransportException;
   
   /**
    * Dempsy will call this method on shutdown.
    */
   public void shutdown();
   
   /**
    * This stats collector should be available to any Sender or Receiver that results
    * from this Transport instance.
    */
   public void setStatsCollector(StatsCollector statsCollector);
}
