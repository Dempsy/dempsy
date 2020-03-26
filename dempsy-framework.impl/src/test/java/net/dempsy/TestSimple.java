package net.dempsy;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Node;
import net.dempsy.lifecycle.simple.MessageProcessor;
import net.dempsy.lifecycle.simple.Mp;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.utils.test.ConditionPoll;

public class TestSimple {

    public static class Dummy implements Receiver {
        NodeAddress dummy = new NodeAddress() {
            private static final long serialVersionUID = 1L;
        };

        @Override
        public void close() {}

        @Override
        public NodeAddress getAddress(final Infrastructure infra) {
            return dummy;
        }

        @Override
        public void start(final Listener<?> listener, final Infrastructure threadingModel) throws MessageTransportException {}
    }

    @Test
    public void testSimple() throws Exception {
        final AtomicLong count = new AtomicLong(0L);

        try (final NodeManager nm = new NodeManager();
            DefaultThreadingModel tm = new DefaultThreadingModel("TB", -1, 1)) {
            final Node n = new Node.Builder("test-app")
                .defaultRoutingStrategyId("net.dempsy.router.simple")
                .receiver(new Dummy())
                .cluster("start")
                .adaptor(new Adaptor() {
                    private Dispatcher disp;
                    boolean done = false;

                    @Override
                    public void stop() {
                        done = true;
                    }

                    @Override
                    public void start() {
                        while(!done) {
                            disp.dispatch(new KeyedMessageWithType(Integer.valueOf(1), "Hello", "string"));
                            // This is here for when the Container has a max pending and it gets starved for CPU cycles
                            // in this particular test.
                            Thread.yield();
                        }
                    }

                    @Override
                    public void setDispatcher(final Dispatcher dispatcher) {
                        this.disp = dispatcher;
                    }
                })
                .cluster("mp")
                .mp(new MessageProcessor(() -> new Mp() {
                    @Override
                    public KeyedMessageWithType[] handle(final KeyedMessage message) {
                        count.incrementAndGet();
                        return null;
                    }
                }, false, "string"))
                .build();

            nm.node(n)
                .collaborator(new LocalClusterSessionFactory().createSession())
                .threadingModel(tm.start());

            nm.start();

            assertTrue(ConditionPoll.poll(o -> count.get() > 100000));
        }
    }

}
