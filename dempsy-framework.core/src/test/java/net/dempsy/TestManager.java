package net.dempsy;

import static net.dempsy.util.Functional.chain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.test.manager.impl2.Factory;
import net.dempsy.test.manager.interf.SomeInterface;
import net.dempsy.utils.test.ConditionPoll;

@RunWith(Parameterized.class)
public class TestManager {
    public static Logger LOGGER = LoggerFactory.getLogger(TestManager.class);

    public static final long RUN_TIME_MILLIS = 2000;
    public static final int NUM_THREADS = 48;

    public final String typeId;
    public final Class<?> expectedClass;
    public final boolean locatorCalled;

    @After
    @Before
    public void reset() {
        Factory.locatorCalled = false;
    }

    public TestManager(final String typeId, final Class<?> expectedClass, final boolean locatorCalled) {
        this.typeId = typeId;
        this.expectedClass = expectedClass;
        this.locatorCalled = locatorCalled;
    }

    @Parameters(name = "{index}: container type={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // {net.dempsy.test.manager.impl1.SomeImpl.class.getPackageName(),net.dempsy.test.manager.impl1.SomeImpl.class,false},
            {net.dempsy.test.manager.impl2.SomeImpl.class.getPackageName(),net.dempsy.test.manager.impl2.SomeImpl.class,true},
            {"my.bogus.typeid",net.dempsy.test.manager.impl3.SomeImpl.class,false},
        });
    }

    @Test
    public void testManager() {
        final Manager<SomeInterface> manager = new Manager<>(SomeInterface.class);
        final SomeInterface retrieved = manager.getAssociatedInstance(typeId);
        assertEquals(expectedClass, retrieved.getClass());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    private <T> Thread[] makeThreads(final int numThreads, final Supplier<Manager<T>> manSupplier, final AtomicBoolean done, final AtomicBoolean failed) {
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

    @Test
    public void testDoubleManager() throws Exception {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Thread[] threads = makeThreads(NUM_THREADS, () -> new Manager<>(SomeInterface.class), done, failed);

        Thread.sleep(RUN_TIME_MILLIS);

        done.set(true);

        Arrays.stream(threads).forEach(t -> assertTrue(ConditionPoll.qpoll(t, o -> !o.isAlive())));

        assertFalse(failed.get());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    @Test
    public void testServiceManager() {

        try (final ServiceManager<SomeInterface> manager = new ServiceManager<>(SomeInterface.class);) {
            manager.start(new TestInfrastructure("testManager", null, null, null));
            final SomeInterface retrieved = manager.getAssociatedInstance(typeId);
            assertEquals(expectedClass, retrieved.getClass());
        }
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

    @Test
    public void testDoubleServiceManager() throws Exception {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Thread[] threads = makeThreads(NUM_THREADS, () -> chain(new ServiceManager<>(SomeInterface.class), s -> s.start(new TestInfrastructure(null))),
            done, failed);

        Thread.sleep(RUN_TIME_MILLIS);

        done.set(true);

        Arrays.stream(threads).forEach(t -> assertTrue(ConditionPoll.qpoll(t, o -> !o.isAlive())));

        assertFalse(failed.get());
        assertEquals(locatorCalled, Factory.locatorCalled);
    }

}
