/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.config;

import java.util.concurrent.TimeUnit;

import net.dempsy.messages.Adaptor;
import net.dempsy.messages.KeySource;
import net.dempsy.messages.MessageProcessorLifecycle;

/**
 * <p>
 * Conceptually, a {@link Cluster} is the collection of all Message Processors that handle the same 'message type' across a
 * an entire application and potentially distributed across a network. It represents a logical step in the processing stream
 * of an application.
 * </p>
 * <p>
 * For example, in the Word Count example from the User Guide, all of the instances of WordCount that represent the step in
 * the chain of processing after the {@link Adaptor} are in the same {@link Cluster}.
 * </p>
 *
 * @see ClusterId
 */
public class Cluster {
    public static final int DEFAULT_MAX_PENDING_MESSAGES_PER_CONTAINER = -1; // infinite (well, limited by the main queue)

    private ClusterId clusterId;
    private MessageProcessorLifecycle<?> mp = null;
    private Adaptor adaptor = null;
    private String routingStrategyId;
    private int maxPendingMessagesPerContainer = DEFAULT_MAX_PENDING_MESSAGES_PER_CONTAINER;
    private String containerTypeId = null;

    private KeySource<?> keySource = null;
    // default to negative cycle time means no eviction cycle runs
    private EvictionFrequency evictionFrequency = new EvictionFrequency(-1L, TimeUnit.DAYS);

    private Object outputExecutor = null;

    public static class EvictionFrequency {
        public final long evictionFrequency;
        public final TimeUnit evictionTimeUnit;

        public EvictionFrequency(final long evictionFrequency, final TimeUnit evictionTimeUnit) {
            this.evictionFrequency = evictionFrequency;
            this.evictionTimeUnit = evictionTimeUnit;
        }
    }

    /**
     * Create a ClusterDefinition from a cluster name. A {@link Cluster} is to be embedded in an {@link ApplicationDefinition} so it only needs to cluster name
     * and not the entire {@link ClusterId}.
     */
    Cluster(final String applicationName, final String clusterName) {
        this.clusterId = new ClusterId(applicationName, clusterName);
    }

    public Cluster(final String clusterName) {
        this.clusterId = new ClusterId(null, clusterName);
    }

    // ============================================================================
    // Builder functionality.
    // ============================================================================
    public Cluster mp(final MessageProcessorLifecycle<?> messageProcessor) throws IllegalStateException {
        if(this.mp != null)
            throw new IllegalStateException("MessageProcessorLifecycle already set on cluster " + clusterId);
        if(this.adaptor != null)
            throw new IllegalStateException("Adaptor already set on cluster " + clusterId + ". Cannot also set a MessageProcessorLifecycle");

        this.mp = messageProcessor;
        return this;
    }

    public Cluster maxPendingMessagesPerContainer(final int maxPendingMessagesPerContainer) {
        this.maxPendingMessagesPerContainer = maxPendingMessagesPerContainer;
        return this;
    }

    public Cluster adaptor(final Adaptor adaptor) throws IllegalStateException {
        if(this.adaptor != null)
            throw new IllegalStateException("Adaptor already set on cluster " + clusterId);
        if(this.mp != null)
            throw new IllegalStateException("MessageProcessorLifecycle already set on cluster " + clusterId + ". Cannot also set an Adaptor");
        this.adaptor = adaptor;
        return this;
    }

    /**
     * Use routingStrategyId()
     *
     * @param routingStrategyId
     * @return
     */
    @Deprecated
    public Cluster routing(final String routingStrategyId) {
        return routingStrategyId(routingStrategyId);
    }

    public Cluster routingStrategyId(final String routingStrategyId) {
        this.routingStrategyId = routingStrategyId;
        return this;
    }

    public Cluster keySource(final KeySource<?> keySource) {
        this.keySource = keySource;
        return this;
    }

    public Cluster evictionFrequency(final long evictionFrequency, final TimeUnit timeUnit) {
        this.evictionFrequency = new EvictionFrequency(evictionFrequency, timeUnit);
        return this;
    }

    public Cluster outputScheduler(final Object outputExecutor) {
        this.outputExecutor = outputExecutor;
        return this;
    }
    // ============================================================================

    // ============================================================================
    // Java Bean functionality
    // ============================================================================
    public KeySource<?> getKeySource() {
        return keySource;
    }

    public void setKeySource(final KeySource<?> keySource) {
        this.keySource = keySource;
    }

    public EvictionFrequency getEvictionFrequency() {
        return evictionFrequency;
    }

    public void setEvictionFrequency(final EvictionFrequency evictionFrequency) {
        this.evictionFrequency = evictionFrequency;
    }

    public int getMaxPendingMessagesPerContainer() {
        return maxPendingMessagesPerContainer;
    }

    public void setMaxPendingMessagesPerContainer(final int maxPendingMessagesPerContainer) {
        maxPendingMessagesPerContainer(maxPendingMessagesPerContainer);
    }

    private Cluster containerTypeId(final String containerTypeId) {
        this.containerTypeId = containerTypeId;
        return this;
    }

    public void setContainerTypeId(final String containerTypeId) {
        containerTypeId(containerTypeId);
    }

    public String getContainerTypeId() {
        return containerTypeId;
    }

    public Object getOutputScheduler() {
        return outputExecutor;
    }

    public void setOutputScheduler(final Object outputScheduler) {
        outputScheduler(outputScheduler);
    }

    /**
     * Get the full clusterId of this cluster.
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * Set the list of explicit destination that outgoing messages should be limited to.
     */
    public void setDestinations(final ClusterId... destinations) {
        throw new UnsupportedOperationException("Setting explicit destinations is currently broken");
        // destination(destinations);
    }

    public String getRoutingStrategyId() {
        return routingStrategyId;
    }

    public void setRoutingStrategyId(final String routingStrategyId) {
        routing(routingStrategyId);
    }

    public void setMessageProcessor(final MessageProcessorLifecycle<?> mp) {
        mp(mp);
    }

    public MessageProcessorLifecycle<?> getMessageProcessor() {
        return mp;
    }

    public void setAdaptor(final Adaptor adaptor) {
        adaptor(adaptor);
    }

    public Adaptor getAdaptor() {
        return adaptor;
    }
    // ============================================================================

    public boolean isAdaptor() {
        return(adaptor != null);
    }

    // This is called from Node
    void setAppName(final String appName) {
        if(clusterId.applicationName != null && !clusterId.applicationName.equals(appName))
            throw new IllegalStateException("Restting the application name on a cluster is not allowed.");
        clusterId = new ClusterId(appName, clusterId.clusterName);
    }

    public void validate() throws IllegalStateException {
        if(mp == null && adaptor == null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype. " +
                clusterId + " doesn't appear to be configure with either.");
        if(mp != null && adaptor != null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype but not both. " +
                clusterId + " appears to be configured with both.");

        if(mp != null)
            mp.validate();

        if(adaptor != null && keySource != null)
            throw new IllegalStateException("A dempsy cluster can not pre-instantation an adaptor.");

        if(routingStrategyId == null && adaptor == null) // null routingStrategyId is fine if we're an adaptor
            throw new IllegalStateException("No routing strategy set for " + clusterId + ". This should be set on the "
                + Cluster.class.getSimpleName() + " or on the " + Node.class.getSimpleName());
    }
}
