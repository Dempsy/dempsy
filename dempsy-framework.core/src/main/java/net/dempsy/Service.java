package net.dempsy;

import java.util.Map;

public interface Service extends AutoCloseable {

    public void start(Infrastructure infra);

    public void stop();

    public boolean isReady();

    @Override
    public default void close() {
        stop();
    }

    /**
     * Helper method for reading constructed {@link Infrastructure} configuration values.
     */
    public default String getConfigValue(final Map<String, String> conf, final String suffix, final String defaultValue) {
        final String entireKey = configKey(suffix);
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    /**
     * Helper method for reading constructed {@link Infrastructure} configuration values.
     */
    public default String configKey(final String suffix) {
        return this.getClass().getPackageName() + "." + suffix;
    }

}
