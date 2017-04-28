package net.dempsy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SafeString;

public class ServiceTracker implements AutoCloseable {
    private static Logger LOGGER = LoggerFactory.getLogger(ServiceTracker.class);

    private final List<AutoCloseable> services = new ArrayList<>();

    public <T extends AutoCloseable> T track(final T service) {
        add(service);
        return service;
    }

    public <T extends Service> T start(final T service, final Infrastructure infra) {
        if (service == null)
            return service;

        add(service);
        service.start(infra);
        return service;
    }

    public void stopAll() {
        final List<AutoCloseable> tmp = new ArrayList<>();
        synchronized (services) {
            tmp.addAll(services);
            services.clear();
        }
        final int len = tmp.size();
        IntStream.range(0, len).map(i -> len - i - 1).forEach(i -> {
            final AutoCloseable ac = tmp.get(i);
            try {
                ac.close();
            } catch (final Exception e) {
                final String message = "Failed to stop the service " + SafeString.objectDescription(ac) + ". Continuing.";
                if (LOGGER.isDebugEnabled())
                    LOGGER.warn("Failed to stop the service " + SafeString.objectDescription(ac) + ". Continuing.", e);
                else LOGGER.warn(message);
            }
        });
    }

    public boolean allReady() {
        synchronized (services) {
            for (final AutoCloseable ac : services) {
                if (Service.class.isAssignableFrom(ac.getClass())) {
                    final boolean isReady = ((Service) ac).isReady();
                    if (!isReady) {
                        if (LOGGER.isTraceEnabled())
                            LOGGER.trace("The Service \"" + ac.getClass().getSimpleName() + "\" isnt' ready.");
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private void add(final AutoCloseable service) {
        if (service != null) {
            synchronized (services) {
                services.add(service);
            }
        }
    }

    @Override
    public void close() throws Exception {
        stopAll();
    }

}
