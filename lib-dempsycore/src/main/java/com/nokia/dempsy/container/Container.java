package com.nokia.dempsy.container;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.KeySource;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.output.OutputInvoker;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.serialization.Serializer;

public interface Container extends Listener, OutputInvoker, RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener
{
   public void setPrototype(Object prototype) throws ContainerException;
   
   public void setDispatcher(Dispatcher dispatcher);
   
   public void setStatCollector(StatsCollector collector);
   
   public void setSerializer(Serializer<Object> serializer);
   
   public void setKeySource(KeySource<?> keySource);
   
   public void setEvictionCheckInterval(long evictionCheckTimeMillis);
   
   public void setExecutor(DempsyExecutor executor);
   
   public void shutdown();
}
