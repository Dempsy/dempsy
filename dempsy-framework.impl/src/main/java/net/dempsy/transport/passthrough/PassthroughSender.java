package net.dempsy.transport.passthrough;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;

public class PassthroughSender implements Sender {
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

        reciever.listener.onMessage(message);
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
