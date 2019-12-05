package net.dempsy.container;

import java.util.List;

public interface MessageDeliveryJob {

    public boolean containersCalculated();

    public ContainerJobMetadata[] containerData();

    public void calculateContainers();

    /**
     * <p>
     * It's possible that the message can be wholly rejected by calling the reject
     * method above. But once 'individuate' has been called, the message can be rejected
     * piecemeal by calling 'reject' on the individual ContainerJob</o>
     */
    public void rejected();

    /**
     * Either executeAllContainers will be called meaning the ThreadingModel has opted
     * to deliver the message to all containers from the current thread in succession,
     * (X)OR the individuate method will be called so that the delivery to the container
     * can be broken up and staged asynchronously.
     */
    public void executeAllContainers();

    /**
     * <p>
     * Either executeAllContainers will be called meaning the ThreadingModel has opted
     * to deliver the message to all containers from the current thread in succession,
     * (X)OR the individuate method will be called so that the delivery to the container
     * can be broken up and staged asynchronously.
     * </p>
     *
     * <p>
     * It's possible that the message can be wholly rejected by calling the reject
     * method above. But once 'individuate' has been called, the message can be rejected
     * piecemeal by calling 'reject' on the individual ContainerJob</o>
     */
    public List<ContainerJob> individuate();

    /**
     * If the job is individuated, then once all of the jobs are finished, this callback
     * will be called from the ThreadingModel.
     */
    public void individuatedJobsComplete();
}
