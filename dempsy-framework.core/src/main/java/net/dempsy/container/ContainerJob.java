package net.dempsy.container;

import java.util.concurrent.Callable;

public interface ContainerJob extends Callable<Object> {

    public boolean containersCalculated();

    public ContainerJobMetadata[] containerData();

    public void calculateContainers();

    public void rejected();
}
