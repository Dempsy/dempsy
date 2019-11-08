package net.dempsy.messages;

public interface MessageResourceManager {

    public void dispose(Object message);

    public Object replicate(Object toReplicate);

    public Object reify(Object message);

}
