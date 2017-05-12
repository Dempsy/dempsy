package net.dempsy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manager<T> {
    private static Logger LOGGER = LoggerFactory.getLogger(Manager.class);

    protected final Map<String, T> registered = new HashMap<>();
    protected final Class<T> clazz;

    public Manager(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public T getAssociatedInstance(final String typeId) throws DempsyException {
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("Trying to find " + clazz.getSimpleName() + " associated with the transport \"{}\"", typeId);

        T ret = null;

        synchronized (registered) {
            ret = registered.get(typeId);
            if (ret == null) {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace(clazz.getSimpleName() + " associated with the id \"{}\" wasn't already registered. Attempting to create one",
                            typeId);

                ret = makeInstance(typeId);
                registered.put(typeId, ret);
            }
        }

        return ret;
    }

    public T makeInstance(final String typeId) {
        // There's an issue opened on Reflections where multi-threaded access to the zip file is broken.
        // see: https://github.com/ronmamo/reflections/issues/81
        final Reflections reflections;
        final Set<Class<? extends T>> senderFactoryClasses;
        synchronized (Reflections.class) {
            // try something stupid like assume it's a package name and the sender factory is in that package
            reflections = new Reflections(typeId + ".");

            senderFactoryClasses = reflections.getSubTypesOf(clazz);
        }

        T ret = null;
        if (senderFactoryClasses != null && senderFactoryClasses.size() > 0) {
            final Class<? extends T> sfClass = senderFactoryClasses.iterator().next();
            if (senderFactoryClasses.size() > 1)
                LOGGER.warn("Multiple " + clazz.getSimpleName() + " implementations in the package \"{}\". Going with {}", typeId,
                        sfClass.getName());

            try {
                ret = sfClass.newInstance();

            } catch (final InstantiationException | IllegalAccessException e) {
                throw new DempsyException(
                        "Failed to create an instance of the " + clazz.getSimpleName() + " \"" + sfClass.getName()
                                + "\". Is there a default constructor?",
                        e, false);
            }
        }

        if (ret == null)
            throw new DempsyException("Couldn't find a " + clazz.getSimpleName() + " registered with transport type id \"" + typeId
                    + "\" and couldn't find an implementing class assuming the transport type id is a package name");
        return ret;
    }

    public void register(final String typeId, final T factory) {
        synchronized (registered) {
            final T oldFactory = registered.put(typeId, factory);

            if (oldFactory != null)
                LOGGER.info("Overridding an already registered " + clazz.getSimpleName() + "  for transport type id {}", typeId);
        }
    }
}
