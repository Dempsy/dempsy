package net.dempsy.config;

import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;

public class ConfigLogger {

    public static void logConfig(final Logger logger, final String propertyName, final Object value, final Object defaultValue) {
        if(defaultValue == null)
            logger.info("CONFIG: {} = {}", propertyName, getValue(propertyName, value, null));
        else
            logger.info("CONFIG: {} = {} the default is {}", propertyName, getValue(propertyName, value, defaultValue), defaultValue);
    }

    public static void logConfig(final Logger logger, final String propertyName, final Object value) {
        logConfig(logger, propertyName, value, null);
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    private static Object getValue(final String name, final Object value, final Object defaultValue) {
        if(value instanceof Function)
            return Optional.ofNullable(((Function)value).apply(name)).orElse(defaultValue + " (defaulted)");
        else
            return Optional.ofNullable(value).orElse(defaultValue + " (defaulted)");
    }

}
