package net.dempsy;

import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;


// TODO: Fix explicit destinations
@Disabled
public class TestExplicitDestinations extends DempsyBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestExplicitDestinations.class);
    public static final int NUM_MESSAGES = 100;

    {
        super.LOGGER = TestExplicitDestinations.LOGGER;
    }

    @MessageType
    public static class ONeeMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String id = UUID.randomUUID().toString();
        public int count = 0;

        public ONeeMessage() {}

        public ONeeMessage(final int count) {
            this.count = count;
        }

        @MessageKey
        public String key() {
            return id;
        }
    }

    public static class ONeeGenerator implements Adaptor {

        Dispatcher dispatcher;
        AtomicBoolean done = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {
            uncheck(() -> latch.await());
            for(int i = 0; i < NUM_MESSAGES; i++) {
                uncheck(() -> dispatcher.dispatchAnnotated(new ONeeMessage()));
            }
            done.set(true);
        }

        @Override
        public void stop() {}

    }

    @Mp
    public static class Mp1 implements Cloneable {
        static Set<Integer> uniqueCounts = new HashSet<>();

        @MessageHandler
        public Object takes(final ONeeMessage gingout) {
            synchronized(uniqueCounts) {
                uniqueCounts.add(gingout.count);
            }
            return new ONeeMessage(gingout.count + 1);
        }

        @Override
        public Mp1 clone() {
            return (Mp1)uncheck(() -> super.clone());
        }
    }

    @Mp
    public static class Mp2 implements Cloneable {
        static Set<Integer> uniqueCounts = new HashSet<>();

        @MessageHandler
        public ONeeMessage takes(final ONeeMessage gingout) {
            synchronized(uniqueCounts) {
                uniqueCounts.add(gingout.count);
            }
            // return new ONeeMessage(gingout.count + 1);
            return null;
        }

        @Override
        public Mp2 clone() {
            return (Mp2)uncheck(() -> super.clone());
        }
    }

    private static void reset() {
        Mp1.uniqueCounts.clear();
        Mp2.uniqueCounts.clear();
    }

    @SuppressWarnings("resource")
    @ParameterizedTest(name = "{index}: routerId={0}, container={1}, cluster={2}, threading={5}, transport={3}/{4}")
    @MethodSource("combos")
    public void testSeparateNodes(final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
        final String threadingModelDescription, final Object threadingModelSource) throws Exception {
        initParams(routerId, containerId, sessCtx, tpid, serType, threadingModelDescription, threadingModelSource);

        final String[][] oneNodePath = new String[][] {
            {"explicit-destinations/adaptor.xml",
                "explicit-destinations/mp1.xml",
                "explicit-destinations/mp2.xml"}
        };

        runCombos("ted-one-node", oneNodePath, ns -> {
            reset();

            final NodeManager manager = ns.nodes.get(0).manager;
            final ClassPathXmlApplicationContext ctx = ns.nodes.get(0).ctx;

            // wait until I can reach the cluster from the adaptor.
            assertTrue(poll(o -> manager.getRouter().allReachable("mp2-cluster").size() == 1));

            final ONeeGenerator adaptor = ctx.getBean(ONeeGenerator.class);
            adaptor.latch.countDown();
            assertTrue(poll(o -> adaptor.done.get()));
            assertEquals(1, Mp1.uniqueCounts.size());
        });
    }
}
