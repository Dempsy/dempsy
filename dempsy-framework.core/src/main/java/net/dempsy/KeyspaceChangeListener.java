package net.dempsy;

import net.dempsy.router.RoutingStrategy.Inbound;

/**
 * Since the responsibility for the portion of the keyspace that this node is responsible for
 * is determined by the Inbound strategy, when that responsibility changes, Dempsy itself
 * needs to be notified.
 */
public interface KeyspaceChangeListener {
    public void keyspaceChanged(boolean less, boolean more, Inbound inbound);
}
