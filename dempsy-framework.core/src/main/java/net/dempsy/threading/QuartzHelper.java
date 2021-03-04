package net.dempsy.threading;

import java.util.concurrent.TimeUnit;

import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public class QuartzHelper {
    public static Trigger getSimpleTrigger(final TimeUnit timeUnit, final int timeInterval) {
        return getSimpleTrigger(timeUnit, timeInterval, false);
    }

    /**
     * Gets the simple trigger.
     *
     * @param timeUnit the time unit
     * @param timeInterval the time interval
     * @return the simple trigger
     */
    public static Trigger getSimpleTrigger(final TimeUnit timeUnit, final int timeInterval, final boolean ignoreMisfires) {
        SimpleScheduleBuilder simpleScheduleBuilder = null;
        simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule();

        switch(timeUnit) {
            case MILLISECONDS:
                simpleScheduleBuilder.withIntervalInMilliseconds(timeInterval).repeatForever();
                break;
            case SECONDS:
                simpleScheduleBuilder.withIntervalInSeconds(timeInterval).repeatForever();
                break;
            case MINUTES:
                simpleScheduleBuilder.withIntervalInMinutes(timeInterval).repeatForever();
                break;
            case HOURS:
                simpleScheduleBuilder.withIntervalInHours(timeInterval).repeatForever();
                break;
            case DAYS:
                simpleScheduleBuilder.withIntervalInHours(timeInterval * 24).repeatForever();
                break;
            default:
                simpleScheduleBuilder.withIntervalInSeconds(1).repeatForever(); // default 1 sec
        }

        if(ignoreMisfires)
            simpleScheduleBuilder.withMisfireHandlingInstructionIgnoreMisfires();

        final Trigger simpleTrigger = TriggerBuilder.newTrigger()
            .withSchedule(simpleScheduleBuilder)
            .build();
        return simpleTrigger;
    }

}
