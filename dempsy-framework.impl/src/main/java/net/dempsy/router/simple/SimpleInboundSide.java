package net.dempsy.router.simple;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.KeyspaceChangeListener;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.utils.PersistentTask;

public class SimpleInboundSide implements RoutingStrategy.Inbound {
    public static final String SIMPLE_SUBDIR = "simple";

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleInboundSide.class);
    private static final long RETRY_TIMEOUT = 500L;

    private ClusterId clusterId;
    private ContainerAddress address;
    private PersistentTask registerer;
    private ClusterInfoSession session;
    private String rootDir;
    private String actualDir = null;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final AtomicBoolean isReady = new AtomicBoolean(false);

    @Override
    public void start(final Infrastructure infra) {
        this.session = infra.getCollaborator();
        this.rootDir = infra.getRootPaths().clustersDir + "/" + clusterId.clusterName;

        this.registerer = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {
                try {
                    // check if we're still here.
                    if (actualDir != null) {
                        // is actualDir still there?
                        if (session.exists(actualDir, null)) {
                            isReady.set(true);
                            return true;
                        }
                    }
                    session.recursiveMkdir(rootDir, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
                    actualDir = session.mkdir(rootDir + "/" + SIMPLE_SUBDIR, address, DirMode.EPHEMERAL_SEQUENTIAL);
                    session.exists(actualDir, this);
                    LOGGER.debug("Registed " + SimpleInboundSide.class.getSimpleName() + " at " + actualDir);
                    isReady.set(true);
                    return true;
                } catch (final ClusterInfoException e) {
                    final String message = "Failed to register " + SimpleInboundSide.class.getSimpleName() + " for cluster " + clusterId
                            + ". Will retry shortly.";
                    if (LOGGER.isTraceEnabled())
                        LOGGER.debug(message, e);
                    else LOGGER.debug(message);
                    return false;
                }
            }

            @Override
            public String toString() {
                return "register " + SimpleInboundSide.class.getSimpleName() + " for cluster " + clusterId;
            }

        };

        isRunning.set(true);
        registerer.process();
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public void setContainerDetails(final ClusterId clusterId, final ContainerAddress address, final KeyspaceChangeListener nothing) {
        this.clusterId = clusterId;
        this.address = address;
    }

    public String getAddressSubdirectory() {
        return actualDir;
    }

    @Override
    public boolean doesMessageKeyBelongToNode(final Object messageKey) {
        return true; // there can be only one
    }

}
