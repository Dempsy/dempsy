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

package com.nokia.dempsy.config;

import java.lang.reflect.Method;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.KeySource;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.internal.util.SafeString;

/**
 * <p>A {@link ClusterDefinition} is part of an {@link ApplicationDefinition}. For a full
 * description of the {@link ClusterDefinition} please see the {@link ApplicationDefinition}
 * documentation.</p>
 * 
 * <p>Note: for ease of use when configuring by hand (not using a dependency injection framework
 * like Spring or Guice) all "setters" (all 'mutators' in general) return the {@link ClusterDefinition}
 * itself for the purpose of chaining.</p>
 */
public class ClusterDefinition
{
   private ClusterId clusterId;
   private String clusterName;

   private Object messageProcessorPrototype;
   private Adaptor adaptor;
   private ClusterId[] destinations = {};
   private Object strategy = null;
   private Object serializer = null;
   private Object statsCollectorFactory = null;
   private OutputSchedule outputScheduler = null;
   private boolean adaptorIsDaemon = false;
   private KeySource<?> keySource = null;
   
   private ApplicationDefinition parent;
   
   /**
    * Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in
    * an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.
    */
   public ClusterDefinition(String clusterName) { this.clusterName = clusterName; }

   /**
    * <p>Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in
    * an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.</p>
    * 
    * <p>This Cluster will represent an {@link Adaptor} cluster. Use this constructor from a dependency injection
    * framework supporting constructor injection.</p>
    * 
    * @see #setAdaptor
    */
   public ClusterDefinition(String clusterName, Adaptor adaptor) { this.clusterName = clusterName; setAdaptor(adaptor); }

   /**
    * <p>Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in
    * an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.</p>
    * 
    * <p>This Cluster will represent a {@link MessageProcessor} cluster. Use this constructor from a dependency injection
    * framework supporting constructor injection.</p>
    * 
    * @see #setMessageProcessorPrototype
    */
   public ClusterDefinition(String clusterName, Object prototype) throws DempsyException 
   {
      this.clusterName = clusterName;
      setMessageProcessorPrototype(prototype); 
   }

   /**
    * Get the full clusterId of this cluster.
    */
   public ClusterId getClusterId() { return clusterId; }
   
   /**
    * If this {@link ClusterDefinition} identifies specific destination for outgoing
    * messages, this will return the list of ids of those destination clusters.
    */
   public ClusterId[] getDestinations() { return destinations; }
   
   /**
    * Set the list of explicit destination that outgoing messages should be limited to.
    */
   public ClusterDefinition setDestinations(ClusterId... destinations)
   {
      this.destinations = destinations;
      return this;
   }
   
   /**
    * Returns true if there are any explicitly defined destinations.
    * @see #setDestinations
    */
   public boolean hasExplicitDestinations() { return this.destinations != null && this.destinations.length > 0; }
   
   public ClusterDefinition setRoutingStrategy(Object strategy) { this.strategy = strategy; return this; }
   public Object getRoutingStrategy() { return strategy == null ? parent.getRoutingStrategy() : strategy; }
   
   protected ClusterDefinition setParentApplicationDefinition(ApplicationDefinition applicationDef) throws DempsyException
   {
      if (clusterName == null)
         throw new DempsyException("You must set the 'clusterName' when configuring a dempsy cluster for the application: " + String.valueOf(applicationDef));
      clusterId = new ClusterId(applicationDef.getApplicationName(),clusterName); 
      parent = applicationDef;
      return this;
   }
   
   public ApplicationDefinition getParentApplicationDefinition() { return parent; }
   
   public Object getSerializer() { return serializer == null ? parent.getSerializer() : serializer; }
   public ClusterDefinition setSerializer(Object serializer) { this.serializer = serializer; return this; }

   public Object getStatsCollectorFactory() {return statsCollectorFactory == null ? parent.getStatsCollectorFactory() : statsCollectorFactory;}
   public ClusterDefinition setStatsCollectorFactory(Object statsCollectorFactory)
   {
      this.statsCollectorFactory = statsCollectorFactory;
      return this;
   }
   
   public ClusterDefinition setOutputSchedule(OutputSchedule outputScheduler)
   {
      this.outputScheduler = outputScheduler;
      return this;
   }
   
   public OutputSchedule getOutputSchedule() { return this.outputScheduler;}
   
   public ClusterDefinition setMessageProcessorPrototype(Object messageProcessor) throws DempsyException
   {
      this.messageProcessorPrototype = messageProcessor;
      return this;
   }
   public Object getMessageProcessorPrototype() { return messageProcessorPrototype; }
   
   public boolean isRouteAdaptorType(){ return (this.getAdaptor() != null); }
   public Adaptor getAdaptor() { return adaptor; }
   public ClusterDefinition setAdaptor(Adaptor adaptor) { this.adaptor = adaptor; return this; }
   public boolean isAdaptorDaemon() { return adaptorIsDaemon; }
   public ClusterDefinition setAdaptorDaemon(boolean isAdaptorDaemon) { this.adaptorIsDaemon = isAdaptorDaemon; return this; }
   public KeySource<?> getKeySource(){ return keySource; }
   public ClusterDefinition setKeySource(KeySource<?> keySource){ this.keySource = keySource; return this; }
   
   @Override
   public String toString()
   {
      StringBuilder sb = new StringBuilder("(");
      sb.append(clusterId == null ? "(unknown cluster id)" : String.valueOf(clusterId));
      Object obj = messageProcessorPrototype == null ? adaptor : messageProcessorPrototype;
      boolean hasBoth = adaptor != null && messageProcessorPrototype != null;
      sb.append(":").append(obj == null ? "(ERROR: no processor or adaptor)" : obj.getClass().getSimpleName());
      if (hasBoth) // if validate has been called then this can't be true
         sb.append(",").append(adaptor.getClass().getSimpleName()).append("<-ERROR");
      if (hasExplicitDestinations())
         sb.append("|destinations:").append(String.valueOf(destinations));
      sb.append(")");
      return sb.toString();
   }
   
   public void validate() throws DempsyException
   {
      if (parent == null)
         throw new DempsyException("The parent ApplicationDefinition isn't set for the Cluster " + 
               SafeString.valueOf(clusterName) + ". You need to initialize the parent ApplicationDefinition prior to validating");
      if (clusterName == null)
         throw new DempsyException("You must set the 'clusterName' when configuring a dempsy cluster for the application.");
      if (messageProcessorPrototype == null && adaptor == null)
         throw new DempsyException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype. " +
               clusterId + " doesn't appear to be configure with either.");
      if (messageProcessorPrototype != null && adaptor != null)
         throw new DempsyException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype but not both. " +
               clusterId + " appears to be configured with both.");
      
      if (messageProcessorPrototype != null)
      {
         if(!messageProcessorPrototype.getClass().isAnnotationPresent(MessageProcessor.class))
            throw new DempsyException("Attempting to set an instance of \"" + 
                  SafeString.valueOfClass(messageProcessorPrototype) + "\" within the " + 
                  ClusterDefinition.class.getSimpleName() + " for \"" + SafeString.valueOf(clusterId) + 
                  "\" but it isn't identified as a MessageProcessor. Please annotate the class.");

         Method[] methods = messageProcessorPrototype.getClass().getMethods();
         
         boolean foundAtLeastOneMethod = false;
         for(Method method: methods)
         {
            if(method.isAnnotationPresent(MessageHandler.class))
            {
               foundAtLeastOneMethod = true;
               break;
            }
         }
         
         if (!foundAtLeastOneMethod)
            throw new DempsyException("No method on the message processor of type \"" +
                  SafeString.valueOfClass(messageProcessorPrototype) + "\" is identified as a MessageHandler. Please annotate the appropriate method using @MessageHandler.");

         int startMethods = 0;
         for(Method method: methods)
         {
            if (method.isAnnotationPresent(Start.class))
            {
               startMethods++;
            }
         }
         if (startMethods > 1)
            throw new DempsyException("Multiple methods on the message processor of type\""
                  + SafeString.valueOf(messageProcessorPrototype) + "\" is identified as a Start method. Please annotate at most one method using @Start.");
      }
      if(adaptor != null && keySource != null)
      {
         throw new DempsyException("A dempsy cluster can not pre-instantation an adaptor.");
      }
   }
}
