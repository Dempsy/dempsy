package net.dempsy;

public interface Service extends AutoCloseable {

    public void start(Infrastructure infra);

    public void stop();

    public boolean isReady();

    @Override
    public default void close() {
        stop();
    }
}
