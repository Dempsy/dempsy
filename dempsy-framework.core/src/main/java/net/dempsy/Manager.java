package net.dempsy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.distconfig.PropertiesReader.VersionedProperties;
import net.dempsy.distconfig.classpath.ClasspathPropertiesReader;

public class Manager<T> {
    private static Logger LOGGER = LoggerFactory.getLogger(Manager.class);

    protected final Map<String, T> registered = new HashMap<>();
    protected final Class<T> clazz;

    public Manager(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public T getAssociatedInstance(final String typeId) throws DempsyException {
        if(LOGGER.isTraceEnabled())
            LOGGER.trace("Trying to find " + clazz.getSimpleName() + " associated with the transport \"{}\"", typeId);

        T ret = null;

        synchronized(registered) {
            ret = registered.get(typeId);
            if(ret == null) {
                if(LOGGER.isTraceEnabled())
                    LOGGER.trace(clazz.getSimpleName() + " associated with the id \"{}\" wasn't already registered. Attempting to create one",
                        typeId);

                ret = makeInstance(typeId);
                registered.put(typeId, ret);
            }
        }

        return ret;
    }

    public T makeInstance(final String typeId) {
        T ret = null;
        // we're going to try a series of things here to locate the appropriate implementation of clazz.

        // 1) First, we're going to see if the typeId refers to a package name and the package
        // contains a class called Factory that implements Locator.
        {
            try {
                @SuppressWarnings("unchecked")
                final Class<T> factoryClass = (Class<T>)Class.forName(typeId + ".Factory");
                if(Locator.class.isAssignableFrom(factoryClass)) {
                    try {
                        final Locator factory = (Locator)factoryClass.getConstructor().newInstance();
                        ret = factory.locate(clazz);
                    } catch(final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        LOGGER.warn("Failed trying to instantiate the Locator {} using a no-arg contructor:", factoryClass.getName(), e);
                    }
                }
            } catch(final ClassNotFoundException cnfe) {
                LOGGER.trace(
                    "While trying the first method to instantiate an instance of the service type {}, the class {}.Factory doesn't appear to be defined.",
                    typeId, typeId);
            } catch(final RuntimeException rte) {
                LOGGER.warn("Unexpected failure when trying to load the class {}.Factory", typeId, rte);
            }
        }

        // 2) Look for the service location properties file on the classpath at META-INF/{typeId} that contains
        // an entry "{clazz.getname()}=full.class.name.of.implementing.class"
        if(ret == null) {
            try {
                final ClasspathPropertiesReader propReader = new ClasspathPropertiesReader("classpath:///META-INF/" + typeId);
                final VersionedProperties props = propReader.read(null);
                if(props.version >= 0) { // otherwise the properties file doesn't exist
                    // see if the properties file contains the appropriate entry.
                    final String classToInstantiateStr = props.getProperty(clazz.getName());
                    if(classToInstantiateStr != null) {
                        try {
                            @SuppressWarnings("unchecked")
                            final Class<T> classToInstantiate = (Class<T>)Class.forName(classToInstantiateStr);
                            ret = classToInstantiate.getConstructor().newInstance();
                        } catch(final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                            LOGGER.warn("Failed trying to instantiate the implementation class {} using a no-arg contructor:", classToInstantiateStr, e);
                        } catch(final ClassNotFoundException cnfe) {
                            LOGGER.warn(
                                "While trying the first method to instantiate an instance of the service type " + typeId
                                    + " using the properties file from the classpath \"classpath:///META-INF/" + typeId
                                    + "\", the class {} doesn't appear to be defined.",
                                classToInstantiateStr, cnfe);
                        } catch(final RuntimeException rte) {
                            LOGGER.warn("Unexpected failure when trying to load the class {}", classToInstantiateStr, rte);
                        }

                    }
                }
            } catch(final IOException ioe) {
                LOGGER.warn("Unexpected failure when trying to read the service locator properties file from the classpath for {}", typeId, ioe);
            }
        }

        // // 3) Assume the typeId is a package name and classpath scan for subclasses of clazz and then
        // // instantiate that.
        // if(ret == null) {
        // final Set<Class<? extends T>> classes;
        //
        // // There's an issue opened on Reflections where multi-threaded access to the zip file is broken.
        // // see: https://github.com/ronmamo/reflections/issues/81
        // synchronized(Manager.class) {
        // // So, this is a TOTAL HACK that makes sure the context classLoader is valid.
        // // In at least one use of this library this call is made from a callback that's made
        // // using JNA from a NATIVE "C" spawned thread which seems to have an invalid context
        // // classloader (or at least one that has NO URLs that make up its classpath). This is
        // // actually fixed by using a separate thread.
        // final AtomicReference<Set<Class<? extends T>>> classesRef = new AtomicReference<>(null);
        // chain(new Thread(() -> {
        // // Assume it's a package name and the sender factory is in that package
        // // classes = new Reflections(typeId + ".", new SubTypesScanner(false)).getSubTypesOf(clazz);
        // try {
        // classesRef.set(new Reflections(new ConfigurationBuilder()
        // .setScanners(new SubTypesScanner())
        // .setUrls(ClasspathHelper.forJavaClassPath())
        // .addUrls(ClasspathHelper.forClassLoader(ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader()))
        // .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix(typeId + "."))))
        // .getSubTypesOf(clazz));
        // } catch(final Throwable rte) {
        // LOGGER.error("Failed to find classes from the package \"" + typeId + "\" that implement/extend " + clazz.getName(), rte);
        // } finally {
        // if(classesRef.get() == null) // if there was any failure, we need to kick out the waiting thread.
        // classesRef.set(new HashSet<>());
        // }
        // }, "Dempsy-Manager-Temporary-Classloading-Thread"), t -> t.start());
        // Set<Class<? extends T>> tmpClasses = null;
        // while((tmpClasses = classesRef.get()) == null)
        // Thread.yield();
        // classes = tmpClasses;
        // }
        //
        // if(classes != null && classes.size() > 0) {
        // final Class<? extends T> sfClass = classes.iterator().next();
        // if(classes.size() > 1)
        // LOGGER.warn("Multiple " + clazz.getSimpleName() + " implementations in the package \"{}\". Going with {}", typeId,
        // sfClass.getName());
        //
        // try {
        // ret = sfClass.newInstance();
        // } catch(final InstantiationException | IllegalAccessException e) {
        // throw new DempsyException(
        // "Failed to create an instance of the " + clazz.getSimpleName() + " \"" + sfClass.getName()
        // + "\". Is there a default constructor?",
        // e, false);
        // }
        // }
        // }

        if(ret == null)
            throw new DempsyException("Couldn't find a " + clazz.getName() + " registered with transport type id \"" + typeId
                + "\" and couldn't find an implementing class assuming the transport type id is a package name");
        return ret;
    }

    public void register(final String typeId, final T factory) {
        synchronized(registered) {
            final T oldFactory = registered.put(typeId, factory);

            if(oldFactory != null)
                LOGGER.info("Overridding an already registered " + clazz.getSimpleName() + "  for transport type id {}", typeId);
        }
    }
}
