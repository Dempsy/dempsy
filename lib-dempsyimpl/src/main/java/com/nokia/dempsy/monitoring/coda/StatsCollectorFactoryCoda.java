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

package com.nokia.dempsy.monitoring.coda;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.tcp.TcpDestination;
import com.nokia.dempsy.messagetransport.tcp.TcpTransport;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.monitoring.StatsCollectorFactory;

/**
 * Create a Stats Collector, appropriate for a given cluster,
 * running at a given destination, using the given reporters.
 * 
 * The underlying StatsCollector is based on the Metrics package
 * by Coda Hale at Yammer. By default, it will always export statistics
 * to JMX.  However, it can also export statistics via a number of
 * periodic reporters.  Support for the console, cvs, Graphite, and
 * Ganglia reporters is included in this factory.
 * 
 * On creation, the factory will make the stats collector "appropriate"
 * for the cluster.  In the case of Graphite and Ganglia, that means adding
 * an appropriate prefix to the statistic names, or an appropriate group name.
 * In both cases a combination of the application, cluster and the receiver
 * endpoint are used to build the string.
 */
public class StatsCollectorFactoryCoda implements StatsCollectorFactory
{
   MetricRegistry registry = new MetricRegistry();
   
   /**
    * If you want more refined control over the way metrics are defined in various 
    * monitoring systems like Ganglia or Graphite the you can implement this interface
    * and provide an instance to the StatsCollectorFactoryCoda
    */
   public static interface MetricNamingStrategy
   {
      /**
       * Create a MetricName to be used by the StatsCollectorCoda to register the metric
       * of the provided name.
       */
      public String createName(ClusterId clusterId, String metricName);
      
      /**
       * This method should be implemented to return the prefix for the naming that
       * applies to Ganglia and Graphite. The StatsCollectorCoda uses a default naming
       * strategy that uses the environment name that's set by StatsCollectorFactoryCoda.setEnvironmentPrefix
       * then a '-', then the IP address of the Node this cluster is running on.
       */
      public String buildPrefix(ClusterId clusterId, Destination destination);
   }
   
   private static Logger logger = LoggerFactory.getLogger(StatsCollectorFactoryCoda.class);
   private List<MetricsReporterSpec> reporters = new ArrayList<MetricsReporterSpec>();
   private String envPrefix = "";
   
   public static final String environmentPrefixSystemPropertyName = "environment.prefix";
   
   private MetricNamingStrategy namer = new MetricNamingStrategy()
   {
      @Override
      public String createName(ClusterId clusterId, String metricName)
      {
         return clusterId.getApplicationName() + "-" + clusterId.getMpClusterName() + ".Dempsy." + metricName;
      }
      
      @Override
      public String buildPrefix(ClusterId clusterId, Destination destination)
      {
         // FIXME
         // This is at best a black art.  We want to differentiate between 
         // stats for one MPContainer and another, when running in local mode,
         // or should we ever allow multiple MPContaner per JVM in the distributed
         // model.  That can be done either via the prefix on the reporter, or
         // via the scope on the metric.  Originally we did it here on the 
         // reporter prefix, which puts it at the front, but moving from 
         // one MetricsRegistry per StatsCollector to the single default
         // MetricsRegistry, it needs to be in the scope, so metrics for
         // multiple MPContaners do not collapse uselessly into a single set
         // of metrics.  For external reporters, such as Graphite or Ganglia,
         // we still want to distinguish between stats from different instances.
         // And on Graphite, one can do aggregation across nodes by setting up
         // the aggregator like this
         // all.<metric>.value = sum *.<metric>.value
         // For both of those reasons we want to identify the machine, if
         // not the process in the prefix.  Ideally we'd use a user friendly
         // machine name like "int22", but reverse lookup doesn't always work,
         // and on a sufficiently ill-configured network dns lookup can hang.
         // Ideally we'd also differentiate by process, but we don't want to
         // dirty up the stored namespace with lots of random port numbers or
         // process ids.  For the moment we'll assume one jvm per machine and
         // ignore the process level.  That may need to be revisited, but 
         // the right answer is to have some sort of instance name that is
         // both unique and persistent across process executions.  So, bottom
         // line, for the moment, the IP address we're listening to, with
         // dashes replacing periods to keep it a single level in the
         // metric namespace hierarchy, is what we'll use, falling back to
         // a constant "local" for non-tcp transports.
         
         // if setEnvironmentPrefix was called then start with that.
         String prefix = envPrefix;

         // if -DenvironmentPrefix is set then we want to use that.
         String sysEnvPrefix = System.getProperty(environmentPrefixSystemPropertyName);
         if (sysEnvPrefix != null)
            prefix = sysEnvPrefix;
         
         if (destination != null)
         {
            if (destination instanceof TcpDestination)
               prefix += ((TcpDestination)destination).getInetAddress().getHostAddress().replaceAll("\\.", "-");
            else
               prefix += destination.toString().replaceAll("\\.", "-");
         }
         else // destination == null
         {
            InetAddress addr = TcpTransport.getInetAddressBestEffort();
            prefix += (addr != null) ? addr.getHostAddress().replaceAll("\\.", "-") : "local";
         }

         if (logger.isTraceEnabled()) logger.trace("setting prefix to \"" + prefix + "\"");
         
         return prefix;
      }
   };

   
   /**
    * Set (replace) the list of reporters to be used
    */
   public void setReporters(List<MetricsReporterSpec> reporters)
   {
      this.reporters = reporters;
   }
   
   /**
    * Get the list of reporters to be used
    */
   public List<MetricsReporterSpec> getReporters()
   {
      return reporters;
   }
   
   public void addReporter(MetricsReporterSpec reporter)
   {
      reporters.add(reporter);
   }
   
   /**
    * Set the environment prefix. For Graphite, use a '.' at the end of the prefix
    * to create another level of the hierarchy that corresponds to the environment.
    */
   public void setEnvironmentPrefix(String envPrefix)
   {
      this.envPrefix = envPrefix;
   }
   
   /**
    * If you want more refined control over the way metrics are defined in various 
    * monitoring systems like Ganglia or Graphite the you can implement the
    * {@link MetricNamingStrategy} interface and provide an instance to this method.
    * Every createdStatsCollector from that point on will register it's metrics
    * according to the given naming strategy.
    */
   public void setNamingStrategy(MetricNamingStrategy namer) { this.namer = namer; }
   
   public MetricNamingStrategy getNamingStrategy() { return namer; }
   
   private static interface Stopper
   {
      public void stop();
   }
   
   private List<Stopper> stoppers = new ArrayList<Stopper>();
   private int numRunningStatsCollectors = 0;

   @Override
   public synchronized StatsCollector createStatsCollector(ClusterId clusterId, Destination listenerDestination) {
      
      StatsCollectorCoda sc = new StatsCollectorCoda(clusterId,registry,namer,this);
      
      if (numRunningStatsCollectors == 0)
      {

         final JmxReporter defreporter = JmxReporter.forRegistry(registry).build();
         defreporter.start();
         
         stoppers.add(new Stopper()
         {
            final JmxReporter jmxreporter = defreporter;
            @Override public void stop() { jmxreporter.stop(); }
         });
      
         for( MetricsReporterSpec spec: reporters)
         {
            try
            {
               switch(spec.getType())
               {
                  case CONSOLE:
                  {
                     final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .build();
                     reporter.start(spec.getPeriod(), spec.getUnit());
                     stoppers.add(new Stopper()
                     {
                        final ConsoleReporter mreporter = reporter;
                        @Override public void stop() { mreporter.stop(); }
                     });
                     break;
                  }
                  case CSV:
                  {
                     final CsvReporter reporter = CsvReporter.forRegistry(registry)
                           .formatFor(Locale.US)
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .build(spec.getOutputDir());
                     reporter.start(spec.getPeriod(), spec.getUnit());
                     stoppers.add(new Stopper()
                     {
                        final CsvReporter mreporter = reporter;
                        @Override public void stop() { mreporter.stop(); }
                     });
                     break;
                  }
                  case GRAPHITE:
                  {
                     final Graphite graphite = new Graphite(new InetSocketAddress(spec.getHostName(), spec.getPortNumber()));
                     final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                           .prefixedWith(namer.buildPrefix(clusterId, listenerDestination))
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .filter(MetricFilter.ALL)
                           .build(graphite);
                     reporter.start(spec.getPeriod(), spec.getUnit());
                     stoppers.add(new Stopper()
                     {
                        final GraphiteReporter mreporter = reporter;
                        @Override public void stop() { mreporter.stop(); }
                     });
                     break;
                  }
                  case GANGLIA:
                  {
                     final GMetric ganglia = new GMetric(spec.getHostName(), spec.getPortNumber(), UDPAddressingMode.UNICAST,1);
                     final GangliaReporter reporter = GangliaReporter.forRegistry(registry)
                           //                     .prefixedWith(namer.buildPrefix(clusterId, listenerDestination))
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .filter(MetricFilter.ALL)
                           .build(ganglia);
                     reporter.start(spec.getPeriod(), spec.getUnit());
                     stoppers.add(new Stopper()
                     {
                        final GangliaReporter mreporter = reporter;
                        @Override public void stop() { mreporter.stop(); }
                     });
                     break;
                  }
               }
            } catch (Exception e) {
               logger.error("Can't initialize Metrics Reporter " + spec.toString(), e);
            }
         }
      }
      
      numRunningStatsCollectors++;
      return sc;
   }
   
   protected synchronized void stop(StatsCollectorCoda sc)
   {
      numRunningStatsCollectors--;
      if (numRunningStatsCollectors == 0)
      {
         for (Stopper stopper : stoppers)
            stopper.stop();
      }
   }
}
