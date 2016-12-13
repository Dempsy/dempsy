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

package net.dempsy.router;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Adaptor;
import net.dempsy.Dempsy;
import net.dempsy.Dempsy.Application.Cluster.Node;
import net.dempsy.DempsyException;
import net.dempsy.Dispatcher;
import net.dempsy.annotations.MessageKey;
import net.dempsy.annotations.MessageProcessor;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ApplicationDefinition;
import net.dempsy.config.ClusterDefinition;
import net.dempsy.config.ClusterId;
import net.dempsy.container.MpContainer;
import net.dempsy.container.internal.AnnotatedMethodInvoker;
import net.dempsy.internal.util.SafeString;
import net.dempsy.messagetransport.Destination;
import net.dempsy.messagetransport.MessageTransportException;
import net.dempsy.messagetransport.Sender;
import net.dempsy.messagetransport.SenderFactory;
import net.dempsy.messagetransport.Transport;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.router.RoutingStrategy.Outbound;
import net.dempsy.serialization.Serializer;

/**
 * <p>
 * This class implements the routing for all messages leaving a node. Please note: This object is meant to be instantiated and manipulated by the {@link Dempsy} orchestrator and not used directly. However, it
 * is important to understand how routing within Dempsy works.
 * </p>
 * 
 * <p>
 * Routing a message to a message processor happens in three stages. Given an {@link ApplicationDefinition} that contains many message processor clusters, messages leaving any one {@link Node} need to be routed
 * to the appropriate message processors in other clusters. The stages are as follows:
 * </p>
 * 
 * <p>
 * <li>Using the message's type information (and {@link ClusterDefinition} if "destinations" are set) determine the cluster that contains the message processor that the message needs to be sent to.</li>
 * 
 * <li>Within the cluster determine the {@link Node} currently responsible for processing that message using the messages key ({@link MessageKey}) and the current {@link RoutingStrategy} for that cluster.</li>
 * 
 * <li>Once the message is sent to the appropriate {@link Node} the {@link MpContainer} is responsible for routing the message to the appropriate {@link MessageProcessor}</li>
 * </p>
 * 
 * <p>
 * As mentioned, if the particular cluster that the node that this Router is instantiated in has explicitly defined destinations, then the message routing will be limited to only those destinations.
 * </p>
 * 
 * <p>
 * A router requires a non-null ApplicationDefinition during construction.
 * </p>
 */
public class Router implements Dispatcher, RoutingStrategy.Outbound.Coordinator {
    private static Logger logger = LoggerFactory.getLogger(Router.class);

    private final AnnotatedMethodInvoker methodInvoker = new AnnotatedMethodInvoker(MessageKey.class);
    private ApplicationDefinition applicationDefinition = null;

    private ConcurrentHashMap<Class<?>, Set<ClusterRouter>> routerMap = new ConcurrentHashMap<Class<?>, Set<ClusterRouter>>();
    // protected for test access
    protected ConcurrentHashMap<Class<?>, Object> missingMsgTypes = new ConcurrentHashMap<Class<?>, Object>();

    private final Set<RoutingStrategy.Outbound> outbounds = new HashSet<RoutingStrategy.Outbound>();

    private ClusterInfoSession mpClusterSession = null;
    private SenderFactory defaultSenderFactory;
    private ClusterId currentCluster = null;
    private StatsCollector statsCollector = null;

    protected Set<Class<?>> stopTryingToSendTheseTypes = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());

    public Router(final ApplicationDefinition applicationDefinition) {
        if (applicationDefinition == null)
            throw new IllegalArgumentException("Can't pass a null applicationDefinition to a " + SafeString.valueOfClass(this));
        this.applicationDefinition = applicationDefinition;
    }

    /**
     * Provide the handle to the cluster factory so that each visible cluster can be reached.
     */
    public void setClusterSession(final ClusterInfoSession factory) {
        mpClusterSession = factory;
    }

    /**
     * @return a reference to the set ClusterInfoSession
     */
    public ClusterInfoSession getClusterSession() {
        return mpClusterSession;
    }

    /**
     * Tell the {@link Router} what the current cluster is. This is typically determined by the {@link Dempsy} orchestrator through the use of the {@link CurrentClusterCheck}.
     */
    public void setCurrentCluster(final ClusterId currentClusterId) {
        this.currentCluster = new ClusterId(currentClusterId);
    }

    /**
     * This sets the default {@link Transport} to use for each cluster a message may be routed to. This can be overridden on a per-cluster basis.
     */
    public void setDefaultSenderFactory(final SenderFactory senderFactory) {
        this.defaultSenderFactory = senderFactory;
    }

    /**
     * This sets the StatsCollector to log messages sent via this dispatcher to.
     */
    public void setStatsCollector(final StatsCollector statsCollector) {
        this.statsCollector = statsCollector;
    }

    /**
     * Prior to the {@link Router} being used it needs to be initialized.
     */
    public void initialize() throws ClusterInfoException, DempsyException {
        // applicationDefinition cannot be null because the constructor checks

        // put all of the cluster definitions into a map for easy lookup
        final Map<ClusterId, ClusterDefinition> defs = new HashMap<ClusterId, ClusterDefinition>();
        for (final ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions())
            defs.put(clusterDef.getClusterId(), clusterDef);

        // now see about the one that we are.
        ClusterDefinition currentClusterDef = null;
        if (currentCluster != null) {
            currentClusterDef = defs.get(currentCluster);
            if (currentClusterDef == null)
                throw new DempsyException("This Dempsy instance seems to be misconfigured. While this VM thinks it's an instance of " +
                        currentCluster
                        + " the application it's configured with doesn't contain this cluster definition. The application configuration consists of: "
                        +
                        applicationDefinition);
        }

        // get the set of explicit destinations if they exist
        final Set<ClusterId> explicitClusterDestinations = (currentClusterDef != null && currentClusterDef.hasExplicitDestinations())
                ? new HashSet<ClusterId>() : null;
        if (explicitClusterDestinations != null)
            explicitClusterDestinations.addAll(Arrays.asList(currentClusterDef.getDestinations()));

        // -------------------------------------------------------------------------------------
        // TODO: This loop will eventually be replaced when the instantiation of the Outbound
        // is driven from cluster information management events (Zookeeper callbacks).
        // -------------------------------------------------------------------------------------
        // if the currentCluster is set and THAT cluster has explicit destinations
        // then those are the only ones we want to consider
        for (final ClusterDefinition clusterDef : applicationDefinition.getClusterDefinitions()) {
            if ((explicitClusterDestinations == null || explicitClusterDestinations.contains(clusterDef.getClusterId()))
                    && !clusterDef.isRouteAdaptorType()) {
                final RoutingStrategy strategy = (RoutingStrategy) clusterDef.getRoutingStrategy();
                final ClusterId clusterId = clusterDef.getClusterId();
                if (strategy == null)
                    throw new DempsyException("Could not retrieve the routing strategy for " + SafeString.valueOf(clusterId));

                // This create will result in a callback on the Router as the Outbound.Coordinator with a
                // registration event. The Outbound may (will) call back on the Router to retrieve the
                // MpClusterSession and register itself with the appropriate cluster.
                outbounds.add(strategy.createOutbound(this, mpClusterSession, clusterId));
            }
        }
        // -------------------------------------------------------------------------------------
    }

    @Override
    public ClusterId getThisClusterId() {
        return currentCluster;
    }

    /**
     * A {@link Router} is also a {@link Dispatcher} that is the instance that's typically injected into {@link Adaptor}s. The implementation of this dispatch routes the message to the appropriate
     * {@link MessageProcessor} in the appropriate {@link ClusterDefinition}
     */
    @Override
    public void dispatch(final Object message) {
        if (message == null) {
            logger.warn("Attempt to dispatch null message.");
            return;
        }

        final List<Object> messages = new ArrayList<Object>();
        getMessages(message, messages);
        for (final Object msg : messages) {
            final Class<?> messageClass = msg.getClass();

            Object msgKeysValue = null;
            try {
                if (!stopTryingToSendTheseTypes.contains(messageClass))
                    msgKeysValue = methodInvoker.invokeGetter(msg);
            } catch (final IllegalArgumentException e1) {
                stopTryingToSendTheseTypes.add(msg.getClass());
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" Please make sure its has a simple getter appropriately annotated: " +
                        e1.getLocalizedMessage()); // no stack trace.
            } catch (final IllegalAccessException e1) {
                stopTryingToSendTheseTypes.add(msg.getClass());
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" Please make sure all annotated getter access is public: " +
                        e1.getLocalizedMessage()); // no stack trace.
            } catch (final InvocationTargetException e1) {
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" due to an exception thrown from the getter: " +
                        e1.getLocalizedMessage(), e1.getCause());
            }

            if (msgKeysValue != null) {
                final Set<ClusterRouter> routers = getRouter(msg.getClass());
                if (routers != null) {
                    for (final ClusterRouter router : routers)
                        router.route(msgKeysValue, msg);
                } else {
                    if (statsCollector != null)
                        statsCollector.messageNotSent();
                    logger.warn("No router found for message type \"" + SafeString.valueOf(msg) +
                            (msg != null ? "\" of type \"" + SafeString.valueOf(msg.getClass()) : "") + "\"");
                }
            } else {
                if (statsCollector != null)
                    statsCollector.messageNotSent();
                logger.warn("Null message key for \"" + SafeString.valueOf(msg) +
                        (msg != null ? "\" of type \"" + SafeString.valueOf(msg.getClass()) : "") + "\"");
            }
        }
    }

    public void stop() {
        // stop the MpClusterSession first so that ClusterRouters wont
        // be notified after their stopped.
        try {
            if (mpClusterSession != null)
                mpClusterSession.stop();
        } catch (final Throwable th) {
            logger.error("Stopping the cluster session " + SafeString.objectDescription(mpClusterSession) + " caused an exception:", th);
        }

        // flatten out then stop all of the ClusterRouters
        final ConcurrentHashMap<Class<?>, Set<ClusterRouter>> map = routerMap;
        routerMap = null;
        final Set<ClusterRouter> routers = new HashSet<ClusterRouter>();
        for (final Collection<ClusterRouter> curRouters : map.values())
            routers.addAll(curRouters);
        for (final ClusterRouter router : routers)
            router.stop();
        for (final RoutingStrategy.Outbound ob : outbounds)
            ob.stop();
    }

    @Override
    public void registerOutbound(final RoutingStrategy.Outbound outbound, final Collection<Class<?>> classes) {
        synchronized (outbound) {
            unregisterOutbound(outbound);

            final ClusterId clusterId = outbound.getClusterId();
            if (classes != null && classes.size() > 0) {
                // find the appropriate ClusterDefinition
                final ClusterDefinition curClusterDef = applicationDefinition.getClusterDefinition(clusterId);

                if (curClusterDef != null) {
                    // create a corresponding ClusterRouter
                    @SuppressWarnings("unchecked")
                    final ClusterRouter clusterRouter = new ClusterRouter((Serializer) curClusterDef.getSerializer(), outbound);

                    for (final Class<?> clazz : classes) {
                        Set<ClusterRouter> cur = Collections.newSetFromMap(new ConcurrentHashMap<ClusterRouter, Boolean>()); // potential
                        final Set<ClusterRouter> tmp = routerMap.putIfAbsent(clazz, cur);
                        if (tmp != null)
                            cur = tmp;
                        cur.add(clusterRouter);
                    }
                } else {
                    logger.error("Couldn't find the ClusterDefinition for " + clusterId + " while registering the Outbound " +
                            SafeString.objectDescription(outbound) + " given the ApplicationDefinition " + applicationDefinition);
                }
            }
        }
    }

    @Override
    public void unregisterOutbound(final RoutingStrategy.Outbound outbound) {
        // we don't want to register and unregister the same Outbound at the same time
        // but we can handle registering and unregistering different Outbound's
        synchronized (outbound) {
            for (final Map.Entry<Class<?>, Set<ClusterRouter>> entry : routerMap.entrySet()) {
                final Set<ClusterRouter> crs = entry.getValue();
                for (final Iterator<ClusterRouter> iter = crs.iterator(); iter.hasNext();) {
                    final ClusterRouter cur = iter.next();
                    if (cur.strategyOutbound == outbound)
                        iter.remove();
                }
                // we're not going to remove a potentially empty set, or purpose.
            }
        }
    }

    @Override
    public void finishedDestination(final Outbound outbound, final Destination destination) {
        // find the ClusterRouter that corresponds to the outbound

        // First, make a uniqe set of ClusterRouters
        final Collection<Set<ClusterRouter>> ssrouters = routerMap.values();
        final Set<ClusterRouter> urouters = new HashSet<Router.ClusterRouter>();
        for (final Set<ClusterRouter> srouters : ssrouters)
            urouters.addAll(srouters);

        // now go through the unique and find the one that corresponds to the outbound.
        for (final ClusterRouter r : urouters) {
            // "is" of identity
            if (r.strategyOutbound == outbound) {
                r.senderFactory.reclaim(destination);
                // There should be only one ClusterRouter for this outbound so we can stop.
                break;
            }
        }
    }

    // This should only be called from tests
    public Set<Outbound> dnuobtuOteg() {
        return outbounds;
    }

    /**
     * This class routes messages within a particular cluster. It is protected for test access only. Otherwise it would be private.
     */
    protected class ClusterRouter {
        private final Serializer serializer;
        private final SenderFactory senderFactory = defaultSenderFactory;
        private final RoutingStrategy.Outbound strategyOutbound;

        private ClusterRouter(final Serializer serializer, final Outbound strategyOutbound) {
            this.strategyOutbound = strategyOutbound;
            this.serializer = serializer;
        }

        /**
         * Returns whether or not the message was actually sent. Doesn't touch the statsCollector
         */
        public boolean route(final Object key, final Object message) {
            boolean messageFailed = true;
            Sender sender = null;
            try {
                final Destination destination = strategyOutbound.selectDestinationForMessage(key, message);

                if (destination == null) {
                    if (logger.isInfoEnabled())
                        logger.info("Couldn't find a destination for " + SafeString.objectDescription(message));
                    if (statsCollector != null)
                        statsCollector.messageNotSent();
                    return false;
                }

                sender = senderFactory.getSender(destination);
                if (sender == null)
                    logger.error("Couldn't figure out a means to send " + SafeString.objectDescription(message) +
                            " to " + SafeString.valueOf(destination) + "");
                else {
                    final byte[] data = serializer.serialize(message);
                    sender.send(data); // the sender is assumed to increment the stats collector.
                    messageFailed = false;
                }
            } catch (final DempsyException e) {
                logger.info("Failed to determine the destination for " + SafeString.objectDescription(message) +
                        " using the routing strategy " + SafeString.objectDescription(strategyOutbound), e);
            } catch (final IOException e) {
                logger.error("Failed to serialize " + SafeString.objectDescription(message) +
                        " using the serializer " + SafeString.objectDescription(serializer), e);
            } catch (final MessageTransportException e) {
                logger.warn("Failed to send " + SafeString.objectDescription(message) +
                        " using the sender " + SafeString.objectDescription(sender), e);
            } catch (final Throwable e) {
                logger.error("Failed to send " + SafeString.objectDescription(message) +
                        " using the serializer " + SafeString.objectDescription(serializer) +
                        "\" and using the sender " + SafeString.objectDescription(sender), e);
            }
            if (messageFailed)
                statsCollector.messageNotSent();
            return !messageFailed;
        }

        private void stop() {
            try {
                if (senderFactory != null)
                    senderFactory.stop();
            } catch (final Throwable th) {
                logger.error("Stopping the sender factory " + SafeString.objectDescription(senderFactory) + " caused an exception:", th);
            }
        }
    } // end ClusterRouter definition.

    protected void getMessages(final Object message, final List<Object> messages) {
        if (message instanceof Iterable) {
            @SuppressWarnings("rawtypes")
            final Iterator it = ((Iterable) message).iterator();
            while (it.hasNext())
                getMessages(it.next(), messages);
        } else
            messages.add(message);
    }

    /**
     * This is protected for test access only. Otherwise it would be private.
     */
    protected Set<ClusterRouter> getRouter(final Class<?> msgType) {
        Set<ClusterRouter> routers = routerMap.get(msgType);
        if (routers == null) {
            if (missingMsgTypes.contains(msgType))
                return null;
            else {
                synchronized (routerMap) {
                    routers = routerMap.get(msgType);
                    if (routers == null) {
                        for (final Class<?> c : routerMap.keySet()) {
                            if (c.isAssignableFrom(msgType)) {
                                routers = routerMap.get(c);
                                routerMap.put(msgType, routers);
                                break;
                            }
                        }
                        if (routers == null)
                            missingMsgTypes.put(msgType, new Object());
                    }
                }
            }
        }

        return routers;
    }

}
