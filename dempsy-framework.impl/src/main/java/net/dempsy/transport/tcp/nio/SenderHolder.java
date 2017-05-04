package net.dempsy.transport.tcp.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.tcp.nio.NioSender.StopMessage;
import net.dempsy.transport.tcp.nio.internal.NioUtils;
import net.dempsy.transport.tcp.nio.internal.NioUtils.ReturnableBufferOutput;

public class SenderHolder {
    public final NioSender sender;

    private boolean previouslyWroteOddNumBufs = false;

    private int numBytesToWrite = 0;
    private final LinkedList<ReturnableBufferOutput> serializedMessages = new LinkedList<>();

    public SenderHolder(final NioSender sender) {
        this.sender = sender;
    }

    private final void add(final ReturnableBufferOutput ob) {
        numBytesToWrite += ob.getPosition();
        serializedMessages.add(ob);
    }

    private final void addBack(final ReturnableBufferOutput ob, final int remaining) {
        numBytesToWrite += remaining;
        serializedMessages.add(ob);
    }

    public final void register(final Selector selector) throws ClosedChannelException {
        sender.channel.register(selector, SelectionKey.OP_WRITE, this);
    }

    public final boolean shouldClose() {
        final Object peek = sender.messages.peek();
        return (peek != null && (peek instanceof StopMessage));
    }

    public final boolean readyToSerialize() {
        final Object peek = sender.messages.peek();
        return peek != null && !(peek instanceof StopMessage);
    }

    public final boolean readyToWrite(final boolean considerMtu) {
        return numBytesToWrite() >= (considerMtu ? sender.getMaxBatchSize() : 1);
    }

    public final int numBytesToWrite() {
        return numBytesToWrite;
    }

    public void trySerialize() throws IOException {
        prepareToWriteBestEffort();
    }

    public boolean writeSomethingReturnDone(final SelectionKey key, final NodeStatsCollector statsCollector) throws IOException {
        prepareToWriteBestEffort();

        if (readyToWrite(false)) { // if we have ANYTHING to write.
            // ==================================================
            // do a write pass
            // ==================================================
            // collect up the ByteBuffers to send.
            final int numBb = serializedMessages.size();
            final ReturnableBufferOutput[] toSendRbos = new ReturnableBufferOutput[numBb];
            final ByteBuffer[] toSend = new ByteBuffer[numBb];
            int curIndex = 0;

            for (final ReturnableBufferOutput c : serializedMessages) {
                toSend[curIndex] = c.getFloppedBb();
                toSendRbos[curIndex] = c;
                curIndex++;
            }

            final SocketChannel channel = (SocketChannel) key.channel();
            channel.write(toSend); // okay, let's see what we have now.

            numBytesToWrite = 0;
            serializedMessages.clear();
            int numBufsCompletelyWritten = 0;
            for (int i = 0; i < toSend.length; i++) {
                final ByteBuffer curBb = toSend[i];
                final ReturnableBufferOutput curRob = toSendRbos[i];
                final int remaining = curBb.remaining();
                if (remaining != 0) {
                    addBack(curRob, remaining);
                } else
                    numBufsCompletelyWritten++;
            }

            // how many messages did we write?
            if (previouslyWroteOddNumBufs)
                numBufsCompletelyWritten++;

            previouslyWroteOddNumBufs = (numBufsCompletelyWritten & 0x1) == 0x1; // is numBufsCompletelyWritten now an odd number?

            final int numMessageDelivered = numBufsCompletelyWritten >> 1;
            for (int i = 0; i < numMessageDelivered; i++)
                statsCollector.messageSent(null);

            return !(readyToWrite(false) || readyToSerialize());

        } else
            return sender.messages.peek() == null;

    }

    private void prepareToWriteBestEffort() throws IOException {
        while (true) {
            if (!readyToWrite(true)) {
                if (readyToSerialize()) {
                    serializeOne();
                    continue;
                } else
                    return;
            } else
                return;
        }
    }

    private boolean serializeOne() throws IOException {
        if (shouldClose())
            return false;

        final Object toSer = sender.messages.poll();
        if (toSer != null) {
            final ReturnableBufferOutput header = NioUtils.get();
            final ReturnableBufferOutput data = NioUtils.get();
            serialize(sender.serializer, toSer, header, data);
            add(header);
            add(data);
            return true;
        }
        return false;

    }

    private static void serialize(final Serializer ser, final Object obj, final ReturnableBufferOutput header, final ReturnableBufferOutput data)
            throws IOException {
        header.reset();
        data.reset();
        ser.serialize(obj, data);
        final int size = data.getPosition();
        if (size > Short.MAX_VALUE) {
            header.writeShort((short) -1);
            header.writeInt(size);
        } else
            header.writeShort((short) size);
    }
}