package net.dempsy.transport.passthrough;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.transport.Sender;

public class PassthroughSender implements Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(PassthroughSender.class);

    private final NodeStatsCollector statsCollector;
    private final PassthroughSenderFactory owner;
    private boolean isRunning = true;
    final PassthroughReceiver reciever;

    PassthroughSender(final PassthroughReceiver r, final NodeStatsCollector sc, final PassthroughSenderFactory owner) {
        this.reciever = r;
        this.statsCollector = sc;
        this.owner = owner;
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        if(!isRunning) {
            if(statsCollector != null)
                statsCollector.messageNotSent();
            throw new MessageTransportException("send called on stopped PassthroughSender");
        }

        try {
            reciever.listener.propogateMessageToNode((RoutedMessage)message, false, null);
        } catch(final RuntimeException rte) {
            LOGGER.error("Unexpected excpetion!", rte);
            throw rte;
        }

        if(statsCollector != null)
            statsCollector.messageSent(message);
    }

    @Override
    public void stop() {
        isRunning = false;
        owner.imDone(this);
    }

    @Override
    public boolean considerMessageOwnsershipTransfered() {
        return true;
    }
}
