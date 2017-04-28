package net.dempsy.router.microshard;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.cluster.DirMode;

public class Transaction implements ClusterInfoWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);
    private static final String txPrefix = "Tx_";

    private final String transactionDirPath;
    private final String transactionPath;
    private final ClusterInfoSession session;
    private final ClusterInfoWatcher proxied;
    private String actualPath = null;

    public Transaction(final String transactionDirPath, final ClusterInfoSession session, final ClusterInfoWatcher proxied) {
        this.transactionDirPath = transactionDirPath;
        this.transactionPath = transactionDirPath + "/" + txPrefix;
        this.session = session;
        this.proxied = proxied;
    }

    public void mkRootDirs() throws ClusterInfoException {
        session.recursiveMkdir(transactionDirPath, null, DirMode.PERSISTENT, DirMode.PERSISTENT);
    }

    public void watch() throws ClusterInfoException {
        LOGGER.trace("Setting Tx watch");
        session.getSubdirs(transactionDirPath, this);
    }

    public void open() throws ClusterInfoException {
        if (actualPath != null) {
            // if (session.exists(actualPath, null)) {
            LOGGER.trace("Already opened transaction {}", actualPath);
            return;
            // } else {
            // LOGGER.debug("Seemed to have lost that transaction {}. Reopening", actualPath);
            // actualPath = null;
            // }
        }

        LOGGER.trace("Opening transaction");
        final String tmp = session.mkdir(transactionPath, null, DirMode.EPHEMERAL_SEQUENTIAL);

        final Collection<String> subdirs = session.getSubdirs(transactionDirPath, this);

        // double check that we're there....
        final String baseName = tmp.substring(tmp.lastIndexOf('/') + 1);
        if (!subdirs.contains(baseName))
            throw new ClusterInfoException("Inconsistent transaction. Created " + tmp + " but it's missing from " + subdirs);

        actualPath = tmp; // don't set the actualPath until we're registered.
        LOGGER.trace("Opened transaction {}", actualPath);
    }

    public void close() throws ClusterInfoException {
        LOGGER.trace("Closing transaction {}", actualPath);

        if (actualPath == null) // we're not open
            return;

        final String tmpPath = actualPath;
        actualPath = null;
        // now close.
        if (session.exists(tmpPath, this))
            session.rmdir(tmpPath); // remove the tx marker.
        else {
            // we need to smack it since something strange happend.
            open(); // should reset actualPath
            if (actualPath == null)
                throw new ClusterInfoException("Failed to open while closing.");
            session.rmdir(actualPath);
        }
        LOGGER.trace("Closed transaction");
    }

    @Override
    public void process() {
        LOGGER.trace("Tx notified.");
        try {
            final Collection<String> subdirs = session.getSubdirs(transactionDirPath, this);
            if (subdirs.size() == 0) {
                LOGGER.trace("Tx is closed so notifying {}", proxied);
                proxied.process();
            }
        } catch (final ClusterInfoException e) {
            throw new DempsyException("Failed to check transaction state.", e);
        }
    }
}
