package net.dempsy.messages;

public class DummyMessageResourceManager implements MessageResourceManager {

    @Override
    public void dispose(final Object message) {}

    @Override
    public Object replicate(final Object toReplicate) {
        return toReplicate;
    }
}
