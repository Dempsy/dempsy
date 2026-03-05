package net.dempsy.threading;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.ignore;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.dempsy.container.Container;
import net.dempsy.container.ContainerJob;
import net.dempsy.container.DummyContainer;
import net.dempsy.container.MessageDeliveryJob;

public class TestThreadingModel {

    public static final int NUM_THREADS = 10;
    public static final int MAX_PENDING = 100000;

    public static boolean waitOnSignal = true;

    public static Stream<Arguments> params() {

        final String threadNameBase = TestThreadingModel.class.getSimpleName() + "-";
        final Supplier<ThreadingModel> dtm = () -> chain(new DefaultThreadingModel(threadNameBase, NUM_THREADS, MAX_PENDING),
            tm -> tm.start("nodeid"));

        // threading model, num threads,
        return Stream.of(
            Arguments.of(dtm,NUM_THREADS,MAX_PENDING),
            Arguments.of((Supplier<ThreadingModel>)() -> chain(new OrderedPerContainerThreadingModel(threadNameBase, MAX_PENDING), tm -> tm.start("nodeid")),1,MAX_PENDING),
            Arguments.of((Supplier<ThreadingModel>)() -> chain(new OrderedPerContainerThreadingModelAlt(threadNameBase, NUM_THREADS, MAX_PENDING),
                tm -> tm.start("nodeid")),1,MAX_PENDING)
        );

    }

    private void submitOne(final ThreadingModel ut, final Container container, final Object waitOnMe, final AtomicLong sequence, final AtomicLong numPending,
        final AtomicLong numRejected, final AtomicLong numCompleted, final AtomicLong numCompletedSuccessfully) {
        waitOnSignal = true;

        ut.submitLimited(new MessageDeliveryJob() {
            // long seq = sequence.getAndIncrement();

            @Override
            public void rejected(final boolean stopping) {
                numRejected.incrementAndGet();
                numCompleted.incrementAndGet();
            }

            @Override
            public void individuatedJobsComplete() {}

            @Override
            public List<ContainerJob> individuate() {
                return List.of(new ContainerJob() {

                    @Override
                    public void reject(final Container container) {
                        numRejected.incrementAndGet();
                        numCompleted.incrementAndGet();
                    }

                    @Override
                    public void execute(final Container container) {
                        numPending.incrementAndGet();
                        synchronized(waitOnMe) {
                            if(waitOnSignal)
                                ignore(() -> waitOnMe.wait());
                        }
                        numPending.decrementAndGet();
                        numCompletedSuccessfully.incrementAndGet();
                        numCompleted.incrementAndGet();
                    }
                });
            }

            @Override
            public void executeAllContainers() {
                synchronized(waitOnMe) {
                    numPending.incrementAndGet();
                    synchronized(waitOnMe) {
                        if(waitOnSignal)
                            ignore(() -> waitOnMe.wait());
                    }
                    numPending.decrementAndGet();
                    numCompletedSuccessfully.incrementAndGet();
                    numCompleted.incrementAndGet();
                }
            }

            @Override
            public boolean containersCalculated() {
                return true;
            }

            @Override
            public Container[] containerData() {
                return new Container[] {
                    container
                };
            }

            @Override
            public void calculateContainers() {}
        });
    }

    @ParameterizedTest(name = "{index}: threading model={0}, num threads={1}, max pending={2}")
    @MethodSource("params")
    public void test(final Supplier<ThreadingModel> utSupplier, final int numThreads, final int maxNumLimited) throws Exception {
        final ThreadingModel ut = utSupplier.get();
        final Container container = new DummyContainer();
        try(final ThreadingModel qctm = ut;) {

            final Object waitOnMe = new Object();
            final AtomicLong numPending = new AtomicLong(0L);
            final AtomicLong sequence = new AtomicLong(0L);
            final AtomicLong numRejected = new AtomicLong(0L);
            final AtomicLong numCompleted = new AtomicLong(0L);
            final AtomicLong numCompletedSuccessfully = new AtomicLong(0L);

            int totalNumSubmitted = 0;
            // submit 1 to busy every worker
            for(int i = 0; i < numThreads; i++) {
                submitOne(ut, container, waitOnMe, sequence, numPending, numRejected, numCompleted, numCompletedSuccessfully);
                totalNumSubmitted++;
            }

            assertTrue(poll(o -> numPending.get() >= numThreads));
            // submit 1 for every spot in the queue
            for(int i = 0; i < maxNumLimited; i++) {
                submitOne(ut, container, waitOnMe, sequence, numPending, numRejected, numCompleted, numCompletedSuccessfully);
                totalNumSubmitted++;
            }

            Thread.sleep(10);
            assertEquals(numThreads, numPending.get());
            assertEquals(maxNumLimited, ut.getNumberLimitedPending());
            // double the pending
            for(int i = 0; i < maxNumLimited; i++) {
                submitOne(ut, container, waitOnMe, sequence, numPending, numRejected, numCompleted, numCompletedSuccessfully);
                totalNumSubmitted++;
            }
            assertEquals(maxNumLimited * 2, ut.getNumberLimitedPending());

            submitOne(ut, container, waitOnMe, sequence, numPending, numRejected, numCompleted, numCompletedSuccessfully);
            totalNumSubmitted++;

            assertTrue(poll(o -> numRejected.get() == 1));
            Thread.sleep(10);
            assertTrue(poll(o -> numRejected.get() == 1));
            assertEquals(numThreads, numPending.get());
            assertEquals(maxNumLimited * 2, ut.getNumberLimitedPending());

            // now let one complete.
            synchronized(waitOnMe) {
                waitOnMe.notify();
            }

            // after letting one go, several things will happen.
            // First, all of the ones greater than the max allowed
            // will be rejected since there's now a thread free to
            // do the rejecting. This means there will be 1 complete
            // and maxNumLimited - 1 rejected.
            //
            // Because we DON'T "vent at the container" the following
            // one will execute and wait on the condition. This means
            // we should end up with 1 completed successfully and all
            // threads pending again.
            assertTrue(poll(o -> numCompletedSuccessfully.get() == 1));
            assertTrue(poll(o -> ut.getNumberLimitedPending() <= maxNumLimited));
            Thread.sleep(10);
            assertEquals(1, numCompletedSuccessfully.get());
            assertEquals(numThreads, numPending.get());

            System.out.println("===================================");
            System.out.println("total messages submitted:" + totalNumSubmitted);
            System.out.println("total messages in execution:" + numPending);
            System.out.println("total messages successfully:" + numCompletedSuccessfully);
            System.out.println("total messages rejected:" + numRejected);
            System.out.println("total messages in waiting:" + ut.getNumberLimitedPending());

            assertEquals(maxNumLimited, ut.getNumberLimitedPending());

            // now if we let them all go, the rest should execute.
            synchronized(waitOnMe) {
                waitOnSignal = false;
                waitOnMe.notifyAll();
            }
            assertTrue(poll(totalNumSubmitted, t -> t - numRejected.get() == numCompletedSuccessfully.get()));
            Thread.sleep(200);
            assertEquals(totalNumSubmitted - numRejected.get(), numCompletedSuccessfully.get());

            System.out.println("===================================");
            System.out.println("total messages submitted:" + totalNumSubmitted);
            System.out.println("total messages in execution:" + numPending);
            System.out.println("total messages successfully:" + numCompletedSuccessfully);
            System.out.println("total messages rejected:" + numRejected);
            System.out.println("total messages in waiting:" + ut.getNumberLimitedPending());

            // and finally, are they all accounted for
            assertEquals(totalNumSubmitted, numCompleted.get());
        }
    }

}
