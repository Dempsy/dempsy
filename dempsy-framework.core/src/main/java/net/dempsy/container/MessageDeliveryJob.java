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
 * <li>One of the following will then happen:</li>
 * <ul>
 * <li>{@link executeAllContainers} - the job is to run on all of the calculated containers
 * sequentially in the current thread.</li>
 * <li>{@link rejected} - the entire job is rejected and will not be executing on any
 * containers.</li>
 * </ul>
 * </ul>
 *
 * <p>
 * If the job will be executed in independent threads for each container then the job
 * must be "individuated," that is, a sub-job ({@link ContainerJob}) is created for each
 * destined container. This lifecycle will be:
 * </p>
 *
 * <ul>
 * <li>{@link #containersCalculated} - the threading model will call this to determine if it
 * has to call {@link #calculateContainers}.</li>
 * <li>{@link calculateContainers} - the threading model will call this method giving the
 * job the opportunity to determine which containers the job is destined for. Often this is
 * where deserialization should happen. That is the expectation of the threading model.</li>
 * <li>{@link individuate} - the job needs to be split up into a task per container the
 * message is destined for. A call to {@code #individuate()} means the messages are considered
 * queued to the container. Any rejection or success after that point will be a container/cluster
 * statistic and not a node statistic.</li>
 * <li>For each individuated {@link ContainerJob} on of the following will be called:</li>
 * <ul>
 * <li>{@link execute} - the job is to be dispatched to that container.</li>
 * <li>{@link rejected} - the job is to be rejected by that container..</li>
 * </ul>
 * </ul>
 */
public interface MessageDeliveryJob {

    /**
     * The threading model will call this to determine if it needs to call
     * {@link #calculateContainers()}
     */
    public boolean containersCalculated();

    /**
     * The threading model will call this when the job should determine what containers
     * this message will be sent to. Often this is where deserialization will happen
     */
    public void calculateContainers();

    /**
     * Fetch the calculated container date. This should only be called by the threading model
     * after {@code #containersCalculated()} and {@code #calculateContainers()}
     */
    public Container[] containerData();

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
