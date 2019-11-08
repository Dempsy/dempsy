package net.dempsy;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.uncheck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Node;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.utils.test.ConditionPoll;

public class TestAlmostSimple {

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

    @MessageType
    public static class Message implements Serializable {

        private static final long serialVersionUID = 1L;

        public int message;

        public Message(final int message) {
            this.message = message;
        }

        @MessageKey("even")
        public String even() {
            return ((message & 0x01) == 0x01) ? "no" : "yes";
        }

        @MessageKey("odd")
        public String odd() {
            return ((message & 0x01) == 0x01) ? "yes" : "no";
        }
    }

    @Mp
    public static class MpEven implements Cloneable {
        public List<MpEven> allMps = new ArrayList<>();
        public List<Message> received = new ArrayList<>();
        public String key = null;

        @Activation
        public void activate(final String evenp) {
            key = evenp;
        }

        @MessageHandler("even")
        public void handle(final Message message) {
            if("yes".equals(key))
                if((message.message & 0x01) == 0x01)
                    System.out.println("HERE!");
            received.add(message);
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return chain(new MpEven(), m -> allMps.add(m));
        }
    }

    @Mp
    public static class MpOdd implements Cloneable {
        public List<MpOdd> allMps = new ArrayList<>();
        public List<Message> received = new ArrayList<>();
        public String key = null;

        @Activation
        public void activate(final String evenp) {
            key = evenp;
        }

        @MessageHandler("even")
        public void handle(final Message message) {
            received.add(message);
        }

        @Override
        public MpOdd clone() throws CloneNotSupportedException {
            return chain(new MpOdd(), m -> allMps.add(m));
        }
    }

    @Test
    public void testSimple() throws Exception {
        final MpEven mpEven = new MpEven();
        final MpOdd mpOdd = new MpOdd();

        final AtomicLong count = new AtomicLong(0);

        try (final DefaultThreadingModel tm = new DefaultThreadingModel("TB", -1, 1);
            final NodeManager nm = new NodeManager();) {
            final Node n = new Node.Builder("test-app")
                .defaultRoutingStrategyId("net.dempsy.router.simple")
                .receiver(new Dummy())
                .cluster("start")
                .adaptor(new Adaptor() {
                    private Dispatcher disp;
                    boolean done = false;
                    int cur = 0;
                    AtomicBoolean exitedLoop = new AtomicBoolean(false);

                    @Override
                    public void stop() {
                        done = true;
                        while(!exitedLoop.get())
                            Thread.yield();
                    }

                    @Override
                    public void start() {
                        try {
                            while(!done) {
                                uncheck(() -> disp.dispatchAnnotated(new Message(cur++)));
                                count.incrementAndGet();
                            }
                        } finally {
                            exitedLoop.set(true);
                        }
                    }

                    @Override
                    public void setDispatcher(final Dispatcher dispatcher) {
                        this.disp = dispatcher;
                    }
                })
                .cluster("mpEven")
                .mp(new MessageProcessor<MpEven>(mpEven))
                .cluster("mpOdd")
                .mp(new MessageProcessor<MpOdd>(mpOdd))
                .build();

            nm.node(n)
                .collaborator(new LocalClusterSessionFactory().createSession())
                .threadingModel(tm.start());

            nm.start();

            assertTrue(ConditionPoll.poll(c -> count.get() > 100000));
        }

        Thread.sleep(2000);

        // make sure the messages were distributed correctly.
        assertEquals(2, mpEven.allMps.size());
        assertEquals(2, mpOdd.allMps.size());

        final MpEven mpEvenYes = mpEven.allMps.stream().filter(m -> "yes".equals(m.key)).findAny().get();
        // all messages here should be even numbers
        assertTrue(mpEvenYes.received.stream()
            // .peek(m -> System.out.println("" + m.message + "," + m.even()))
            .allMatch(m -> (m.message & 0x01) == 0));
    }

}
