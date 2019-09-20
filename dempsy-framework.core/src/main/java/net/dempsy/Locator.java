package net.dempsy;

public interface Locator {

    public <T> T locate(Class<T> clazz);

}
