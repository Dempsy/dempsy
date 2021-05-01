package net.dempsy.output;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import net.dempsy.container.Container;
import net.dempsy.container.altnonlocking.NonLockingAltContainer;
import net.dempsy.util.TestInfrastructure;

public class TestOutputSchedulers {

    /** The mp container mock. */
    Container container;
    AtomicBoolean outputInvoked = new AtomicBoolean(false);

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        outputInvoked.set(false);

        // initializing
        container = new NonLockingAltContainer() {

            @Override
            public void invokeOutput() {
                outputInvoked.set(true);
            }
        };
    }

    /**
     * Test relative schedule.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRelativeSchedule() throws Exception {
        try(final RelativeOutputSchedule relativeOutputSchedule = new RelativeOutputSchedule(1, TimeUnit.SECONDS);) {
            relativeOutputSchedule.setOutputInvoker(container);
            relativeOutputSchedule.start(new TestInfrastructure(null));
            assertTrue(poll(outputInvoked, oi -> oi.get()));
        }
    }

    /**
     * Test relative schedule with concurrency.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRelativeScheduleWithConcurrency() throws Exception {
        try(final RelativeOutputSchedule relativeOutputSchedule = new RelativeOutputSchedule(1, TimeUnit.SECONDS);) {
            relativeOutputSchedule.setOutputInvoker(container);
            relativeOutputSchedule.start(new TestInfrastructure(null));
            assertTrue(poll(outputInvoked, oi -> oi.get()));
        }
    }

    /**
     * Test cron schedule.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCronSchedule() throws Exception {
        try(final CronOutputSchedule cronOutputSchedule = new CronOutputSchedule("0/1 * * * * ?");) {
            cronOutputSchedule.setOutputInvoker(container);
            cronOutputSchedule.start(new TestInfrastructure(null));
            assertTrue(poll(outputInvoked, oi -> oi.get()));
        }
    }

    /**
     * Test cron schedule with concurrency setting
     *
     * @throws Exception the exception
     */
    @Test
    public void testCronScheduleWithConcurrencySetting() throws Exception {
        try(final CronOutputSchedule cronOutputSchedule = new CronOutputSchedule("0/1 * * * * ?");) {
            cronOutputSchedule.setOutputInvoker(container);
            cronOutputSchedule.start(new TestInfrastructure(null));
            Thread.sleep(1000);
            assertTrue(poll(outputInvoked, oi -> oi.get()));
        }
    }

}
