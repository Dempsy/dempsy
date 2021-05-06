package net.dempsy.container;

import java.util.List;

/**
 * <p>
 * This represents a job sent to the threading model. Depending on the particular
 * threading model it can operate in one of two modes. A "job" represents a single
 * message but that message can have multiple destinations (containers) within
 * the node. Each of those destinations can be executed in the same thread, or
 * in different threads. If they are to be executed in the same thread then the
 * lifecycle of the message will be as follows:
 * </p>
 *
 * <ul>
 * <li>{@link #containersCalculated} - the threading model will call this to determine if it
 * has to call {@link #calculateContainers}.</li>
 * <li>{@link calculateContainers} - the threading model will call this method giving the
 * job the opportunity to determine which containers the job is destined for. Often this is
 * where deserialization should happen. That is the expectation of the threading model.</li>
 * <li>{@link executeAllContainers} - the job is to run on all of the calculated containers
 * sequentially in the current thread.</li>
 * <li>{@link rejected} - the entire job is rejected and will not be executing on any
 * containers.</li>
 * </ul>
 *
 * <p>
 * If the job will be executed in independent threads for each container then the job
 * must be "individuated," that is, a sub-job ({@link ContainerJob}) is created for each
 * destined container. This lifecycle will be:
 * </p>
 *
 * <ul>
 * <li></li>
 * </ul>
 *
 */
public interface MessageDeliveryJob {

    /**
     * The threading model will call this to determine if it needs to call
     * {@link #calculateContainers()}
     */
    public boolean containersCalculated();

    public ContainerJobMetadata[] containerData();

    /**
     * The threading model will call this when the job should determine what containers
     * this message will be sent to. Often this is where deserialization will happen
     */
    public void calculateContainers();

    /**
     * <p>
     * It's possible that the message can be wholly rejected by calling the reject
     * method above. But once 'individuate' has been called, the message can be rejected
     * piecemeal by calling 'reject' on the individual ContainerJob</o>
     */
    public void rejected(boolean stopping);

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
