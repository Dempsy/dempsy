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

package net.dempsy.monitoring.coda;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Value Object to hold paramters for various Metrics Reporter's
 * setup arguments.
 */
public class MetricsReporterSpec {

	private MetricsReporterType type;
	private TimeUnit unit;
	private long period;
	private String hostName;
	private int portNumber;
	private File outputDir;
	
	/**
	 * Set the type of report to issue.
	 * @see MetricsReporterType
	 * 
	 */
	public void setType(MetricsReporterType type)
	{
		this.type = type;
	}
	
	/**
	 * Get the type of report to issue
	 */
	public MetricsReporterType getType()
	{
		return this.type;
	}
	
	/**
	 * Set the time unit to measure the period in
	 * @see TimeUnit
	 */
	public void setUnit(TimeUnit unit)
	{
		this.unit = unit;
	}
	
	/**
	 * Get the time unit in which the period is measured in
	 * 
	 */
	public TimeUnit getUnit()
	{
		return unit;
	}
	
	/**
	 * Set the period to reports statistics in.
	 * In conjunction with the TimeUnit set,
	 * controls how often statistics are published. 
	 */
	public void setPeriod(long period)
	{
		this.period = period;
	}
	
	/**
	 * GEt the period in which statistics are reported
	 */
	public long getPeriod()
	{
		return period;
	}
	
	/**
	 * Set the hostname to publish statistics to.
	 * This is only useful for the Graphite and Ganglia Reporters
	 */
	public void setHostName(String hostName)
	{
		this.hostName = hostName;
	}
	
	/**
	 * Get the host name statistics are published to
	 */
	public String getHostName()
	{
		return hostName;
	}
	
	/**
	 * Set the port statistics are published to.
	 * Like the hostname, this is only useful for network enabled reporters,
	 * i.e. Graphite and Ganglia. 
	 */
	public void setPortNumber(int portNumber)
	{
		this.portNumber = portNumber;
	}
	
	/**
	 * get the port statistics are published to
	 * 
	 */
	public int getPortNumber()
	{
		return portNumber;
	}
	
	/**
	 * Set the directory statistics are published to.
	 * Only useful for the CSV reporter that writes file there.
	 */
	public void setOutputDir(File outputDir)
	{
		this.outputDir = outputDir;
	}
	
	/**
	 *  Get the directory statistics are published to.
	 */
	public File getOutputDir()
	{
		return outputDir;
	}
}
