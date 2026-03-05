package net.dempsy;

import static net.dempsy.util.Functional.chain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.test.manager.impl2.Factory;
import net.dempsy.test.manager.interf.SomeInterface;
import net.dempsy.utils.test.ConditionPoll;

public class TestManager {
    private static Logger LOGGER = LoggerFactory.getLogger(TestManager.class);

    public static final long RUN_TIME_MILLIS = 2000;
    public static final int NUM_THREADS = 48;

    @AfterEach
    @BeforeEach
    public void reset() {
        Factory.locatorCalled = false;
    }

    public static Stream<Arguments> data() {
        return Stream.of(
            // Arguments.of(net.dempsy.test.manager.impl1.SomeImpl.class.getPackageName(),net.dempsy.test.manager.impl1.SomeImpl.class,false),
            Arguments.of(net.dempsy.test.manager.impl2.SomeImpl.class.getPackageName(),net.dempsy.test.manager.impl2.SomeImpl.class,true),
            Arguments.of("my.bogus.typeid",net.dempsy.test.manager.impl3.SomeImpl.class,false)
        );
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testManager(final String typeId, final Class<?> expectedClass, final boolean locatorCalled) {
        Factory.locatorCalled = false;
        final Manager<SomeInterface> manager = new Manager<>(SomeInterface.class);
        final SomeInterface retrieved = manager.getAssociatedInstance(typeId);
        assertEquals(expectedClass, retrieved.getClass());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    private <T> Thread[] makeThreads(final int numThreads, final String typeId, final Supplier<Manager<T>> manSupplier, final AtomicBoolean done, final AtomicBoolean failed) {
        final Thread[] ret = new Thread[numThreads];
        for(int i = 0; i < numThreads; i++) {
            final Manager<T> manager = manSupplier.get();
            ret[i] = chain(new Thread(() -> {
                while(!done.get()) {
                    try {
                        final T retrieved1 = manager.getAssociatedInstance(typeId);
                        assertTrue(retrieved1 instanceof SomeInterface);
                    } catch(final Exception e) {
                        LOGGER.error("Failed", e);
                        failed.set(true);
                    }
                }
            }), t -> t.start());
        }
        return ret;
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testDoubleManager(final String typeId, final Class<?> expectedClass, final boolean locatorCalled) throws Exception {
        Factory.locatorCalled = false;

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Thread[] threads = makeThreads(NUM_THREADS, typeId, () -> new Manager<>(SomeInterface.class), done, failed);

        Thread.sleep(RUN_TIME_MILLIS);

        done.set(true);

        Arrays.stream(threads).forEach(t -> assertTrue(ConditionPoll.qpoll(t, o -> !o.isAlive())));

        assertFalse(failed.get());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testServiceManager(final String typeId, final Class<?> expectedClass, final boolean locatorCalled) {
        Factory.locatorCalled = false;

        try (final ServiceManager<SomeInterface> manager = new ServiceManager<>(SomeInterface.class);) {
            manager.start(new TestInfrastructure("testManager", null, null, null));
            final SomeInterface retrieved = manager.getAssociatedInstance(typeId);
            assertEquals(expectedClass, retrieved.getClass());
        }
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    @ParameterizedTest(name = "{index}: container type={0}")
    @MethodSource("data")
    public void testDoubleServiceManager(final String typeId, final Class<?> expectedClass, final boolean locatorCalled) throws Exception {
        Factory.locatorCalled = false;

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Thread[] threads = makeThreads(NUM_THREADS, typeId, () -> chain(new ServiceManager<>(SomeInterface.class), s -> s.start(new TestInfrastructure(null))),
            done, failed);

        Thread.sleep(RUN_TIME_MILLIS);

        done.set(true);

        Arrays.stream(threads).forEach(t -> assertTrue(ConditionPoll.qpoll(t, o -> !o.isAlive())));

        assertFalse(failed.get());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

}
