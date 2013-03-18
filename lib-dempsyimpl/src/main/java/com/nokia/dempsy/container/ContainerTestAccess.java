package com.nokia.dempsy.container;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.serialization.Serializer;

public interface ContainerTestAccess
{
   // called from tests
   public Object getPrototype();
   
   // called from tests
   public int getProcessorCount();
   
   // called from tests
   public Serializer<Object> getSerializer();
   
   public Dispatcher getDispatcher();

   public StatsCollector getStatsCollector();

   /**
    *  Returns the Message Processor that is associated with a given key,
    *  <code>null</code> if there is no such MP. Does <em>not</em> create
    *  a new MP.
    *  <p>
    *  <em>This method exists for testing; don't do anything stupid</em>
    */
   public Object getMessageProcessor(Object key);
}
