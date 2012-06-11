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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.internal.util.SafeString;

/**
 * <p>Configuration should be thought of as an xpath with {@link ApplicationDefinition} 
 * at the root element and {@link ClusterDefinition}s as the child elements. It's used
 * to layout the topology of an entire Dempsy appliation. Let's say the following simple 
 * processing pipeline constitutes a Dempsy application:</p>
 * <pre>
 * <code>
 *      SimpleAdaptor -> SimpleMessageProcessor
 * </code>
 * </pre>
 * <p>The straightforward configuration that accepts all of the defaults would be:</p>
 * <pre>
 * <code>
 * new ApplicationDefinition("my-application").add(
 *   new ClusterDefinition("adaptor-stage",new SimpleMessageAdaptor()),
 *   new ClusterDefinition("processor-stage",new SimpleMessageProcessor()));
 * </code>
 * </pre>
 * <p>As an example using Spring, your application context would contain:</p>
 * <pre>
 * <code>
 * &lt;bean class="com.nokia.dempsy.config.ApplicationDefinition"&gt;
 *    &lt;property name="applicationName" value="my-application" /&gt;
 *    &lt;property name="clusterDefinitions"&gt;
 *       &lt;list&gt;
 *          &lt;bean class="com.nokia.dempsy.config.ClusterDefinition"&gt;
 *             &lt;constructor-arg name="clusterName" value="adaptor-stage" /&gt;
 *             &lt;property name="adaptor" &gt;
 *                &lt;bean class="SimpleMessageAdaptor" /&gt;
 *             &lt;/property&gt;
 *          &lt;/bean&gt;
 *          &lt;bean class="com.nokia.dempsy.config.ClusterDefinition"&gt;
 *             &lt;constructor-arg name="clusterName" value="processor-stage" /&gt;
 *             &lt;property name="messageProcessorPrototype"&gt;
 *                &lt;bean class="SimpleMessageProcessor" /&gt;
 *             &lt;/property&gt;
 *          &lt;/bean&gt;
 *       &lt;/list&gt;
 *    &lt;/property&gt;
 * &lt;/bean&gt;
 * </code>
 * </pre>
 * 
 * <p>Notice, when using a dependency injection framework, you have the ability to configure
 * your prototypes ({@link MessageProcessor}s) and {@link Adaptor}s  independently from
 * Dempsy. The Dempsy {@link ApplicationDefinition} would then be the point where you define
 * the topology of processing chain. This nicely decouples the two concerns.</p>
 * 
 * <p>While any DI framework can be used if you then join the {@link ApplicationDefinition} to
 * a Dempy instance (by hand), this is unnecessary when using either Spring or Guice as both
 * are supported out-of-the-box.</p>
 * 
 * <p>Note: for ease of use when configuring by hand (not using a dependency injection framework
 * like Spring or Guice) all "setters" (all 'mutators' in general) return the {@link ApplicationDefinition}
 * itself for the purpose of chaining.</p>
 */
public class ApplicationDefinition
{
   private List<ClusterDefinition> clusterDefinitions = new ArrayList<ClusterDefinition>();
   private String applicationName = null;
   private boolean isInitialized = false;
   private Object serializer;
   private Object routingStrategy;
   private Object statsCollectorFactory;
   
   public ApplicationDefinition(String applicationName) { this.applicationName = applicationName; }

 //------------------------------------------------------------------------
 // Simple bean attributes/injection points
 //------------------------------------------------------------------------
   
   /**
    * Get the currently set application name for this {@link ApplicationDefinition}.
    */
   public String getApplicationName() { return applicationName; }

   /**
    * Provides for a dependency injection framework's, setter injection of the {@link ClusterDefinition}s
    */
   public ApplicationDefinition setClusterDefinitions(List<ClusterDefinition> clusterDefs)
   {
      this.clusterDefinitions.clear();
      this.clusterDefinitions.addAll(clusterDefs);
      isInitialized = false;
      return this;
   }
   
   /**
    * Get the currently set {@link ClusterDefinition}s for this {@link ApplicationDefinition}.
    */
   public List<ClusterDefinition> getClusterDefinitions() { return Collections.unmodifiableList(clusterDefinitions); }
   
   /**
    * Get the {@link ClusterDefinition} that corresponds to the given clusterId.
    */
   public ClusterDefinition getClusterDefinition(ClusterId clusterId)
   {
      for (ClusterDefinition cur : clusterDefinitions)
         if (cur.getClusterId().equals(clusterId))
            return cur;
      return null;
   }
   
   /**
    * When configuring by hand, this method 
    * @param clusterDefinitions is the {@link ClusterDefinition}s that make
    *  up this Application.
    * @return the current ApplicationDefinition to allow for chaining.
    */
   public ApplicationDefinition add(ClusterDefinition... clusterDefinitions)
   {
      this.clusterDefinitions.addAll(Arrays.asList(clusterDefinitions));
      isInitialized = false;
      return this;
   }
   
   /**
    * Get the currently overridden serializer for this {@link ApplicationDefinition}
    */
   public Object getSerializer() { return serializer; }
   
   /**
    * Override the default serializer for this {@link ApplicationDefinition}. Note
    * the Object passed must be an instance of {@code com.nokia.dempsy.serializer.Serializer}
    */
   public ApplicationDefinition setSerializer(Object serializer) { this.serializer = serializer; return this; }

   /**
    * Get the currently overridden RoutingStrategy for this {@link ApplicationDefinition}
    */
   public Object getRoutingStrategy() { return routingStrategy; }

   /**
    * Override the default RoutingStrategy for this {@link ApplicationDefinition}. Note
    * the Object passed must be an instance of {@code com.nokia.dempsy.routing.RoutingStrategy}.
    * Note that RoutingStrategy overriding is not for the faint of heart. We plan
    * on eventually providing a standard means of selected between out-of-the-box
    * RoutingStrategys.
    */
   public ApplicationDefinition setRoutingStrategy(Object routingStrategy) { this.routingStrategy = routingStrategy; return this; }

   /**
    * Get the currently overridden StatsCollector for this {@link ApplicationDefinition}
    */
   public Object getStatsCollectorFactory() {return statsCollectorFactory; }
   
   /**
    * Override the default StatsCollector for this {@link ApplicationDefinition}
    * Note the Object passed must be an instance of {@code com.nokia.dempsy.monitoring.StatsCollectorFactory}.
    * While this could be used to completely replace the implementation of the {@link StatsCollector},
    * a more common use case would be to define one or more {@link MetricsReporterSpec}s to the default factory.
    * in order to emit statistics to monitors like Graphite or Ganglia.
    */
   public ApplicationDefinition setStatsCollectorFactory(Object statsCollectorFactory)
   {
	   this.statsCollectorFactory = statsCollectorFactory;
	   return this;
   }
//------------------------------------------------------------------------
// Rudimentary functionality
//------------------------------------------------------------------------
   
   /**
    * This is called from the Dempsy framework itself so there is no need for the
    * user to call it.
    */
   public void initialize() throws DempsyException
   {
      if (!isInitialized)
      {
         for (ClusterDefinition clusterDef : clusterDefinitions)
         {
            if (clusterDef == null)
               throw new DempsyException("The application definition for \"" + applicationName + "\" has a null ClusterDefinition.");

            clusterDef.setParentApplicationDefinition(this);
         }
      
         isInitialized = true;
         validate();
      }
   }
   
   @Override
   public String toString()
   {
      StringBuilder ret = new StringBuilder("application(");
      ret.append(String.valueOf(applicationName)).append("):[");
      boolean first = true;
      for (ClusterDefinition clusterDef : getClusterDefinitions())
      {
         if (!first)
            ret.append(",");
         ret.append(String.valueOf(clusterDef));
         first = false;
      }
      ret.append("]");
      return ret.toString();
   }
   
   public void validate() throws DempsyException
   {
      initialize();
      
      if (applicationName == null)
         throw new DempsyException("You must set the application name while configuring a Dempsy application.");
      
      if (clusterDefinitions == null || clusterDefinitions.size() == 0)
         throw new DempsyException("The application \"" + SafeString.valueOf(applicationName) + "\" doesn't have any clusters defined.");
      
      Set<ClusterId> clusterNames = new HashSet<ClusterId>();
         
      for (ClusterDefinition clusterDef : clusterDefinitions)
      {
         if (clusterDef == null)
            throw new DempsyException("The application definition for \"" + applicationName + "\" has a null ClusterDefinition.");
         
         if (clusterNames.contains(clusterDef.getClusterId()))
            throw new DempsyException("The application definition for \"" + applicationName + "\" has two cluster definitions with the name \"" + clusterDef.getClusterId().getMpClusterName() + "\"");
         
         clusterNames.add(clusterDef.getClusterId());
         
         clusterDef.validate();
      }
   }
   
}
