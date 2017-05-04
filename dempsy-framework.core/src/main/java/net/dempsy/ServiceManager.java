package net.dempsy;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceManager<T extends Service> extends Manager<T> implements Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceManager.class);
    protected Infrastructure infra;
    boolean isRunning = true;

    public ServiceManager(final Class<T> clazz) {
        super(clazz);
    }

    @Override
    public T getAssociatedInstance(final String typeId) throws DempsyException {
        if (infra == null)
            throw new IllegalStateException(
                    "Cannot instantiate service of type " + typeId + " prior to " + this.getClass().getSimpleName() + " being started.");
        T ret = null;

        synchronized (registered) {
            if (!isRunning)
                throw new IllegalStateException("getAssociatedInstance called on a stopped " + this.getClass().getSimpleName());
            ret = registered.get(typeId);

            if (ret == null) {
                ret = super.getAssociatedInstance(typeId);
                ret.start(infra);
            } else {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace("Trying to find " + clazz.getSimpleName() + " associated with the transport \"{}\"", typeId);
            }
            return ret;
        }
    }

    @Override
    public void start(final Infrastructure infra) {
        this.infra = infra;
    }

    @Override
    public void stop() {
        final List<T> tmp = new ArrayList<>();
        synchronized (registered) {
            isRunning = false;
            tmp.addAll(registered.values());
            registered.clear();
        }

        tmp.forEach(rsf -> {
            try {
                rsf.stop();
            } catch (final Exception ret) {
                LOGGER.warn("Failed to shut down an instance of " + clazz.getSimpleName(), ret);
            }
        });

        if (infra != null) {
            infra.close();
        }
    }

    @Override
    public boolean isReady() {
        if (infra == null)
            return false;
        final List<T> tmp = new ArrayList<>();
        synchronized (registered) {
            tmp.addAll(registered.values());
        }
        for (final Service cur : tmp) {
            if (!cur.isReady())
                return false;
        }
        return true;
    }

}
