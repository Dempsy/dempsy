package net.dempsy.container;

public interface ContainerJob {

    public void execute(ContainerJobMetadata container);

    public void reject(ContainerJobMetadata container);
}
