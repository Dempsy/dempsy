package net.dempsy.transport.passthrough;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.dempsy.Infrastructure;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;

public class PassthroughSenderFactory implements SenderFactory {
    private NodeStatsCollector statsCollector = null;

    Map<NodeAddress, PassthroughSender> senders = new HashMap<>();

    @Override
    public synchronized void stop() {
        final List<PassthroughSender> snapshot = new ArrayList<>(senders.values());
        snapshot.forEach(s -> s.stop());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public synchronized PassthroughSender getSender(final NodeAddress destination) throws MessageTransportException {
        PassthroughSender ret = senders.get(destination);
        if (ret == null) {
            final PassthroughReceiver r = PassthroughReceiver.receivers.get(Long.valueOf(((PassthroughAddress) destination).destinationId));
            if (r == null)
                throw new MessageTransportException("Recveiver for " + destination + " is shut down");
            ret = new PassthroughSender(r, statsCollector, this);
            senders.put(destination, ret);
        }
        return ret;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.statsCollector = infra.getNodeStatsCollector();
    }

    synchronized void imDone(final PassthroughSender sender) {
        senders.remove(sender.reciever.getAddress());
    }

}
