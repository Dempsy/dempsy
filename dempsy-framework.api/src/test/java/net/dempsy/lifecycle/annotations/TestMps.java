package net.dempsy.lifecycle.annotations;

import java.util.List;

import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.BulkMessageHandler;
import net.dempsy.lifecycle.annotation.Evictable;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Passivation;

public class TestMps {
    @Mp
    public static class TestMp implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final String key, final byte[] data) {
            this.activated = true;
        }

        @Passivation
        public byte[] passivate() {
            passivateCalled = true;
            return "passivate".getBytes();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpActivateWithMessage implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;
        public Message message = null;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final String key, final byte[] data, final Object message) {
            this.activated = true;
            this.message = (Message)message;
        }

        @Passivation
        public byte[] passivate() {
            passivateCalled = true;
            return "passivate".getBytes();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpEmptyActivate implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate() {
            this.activated = true;
        }

        @Passivation
        public void passivate() {
            passivateCalled = true;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpOnlyKey implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final String key) {
            this.activated = true;
        }

        @Passivation
        public byte[] passivate() {
            passivateCalled = true;
            return "passivate".getBytes();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpExtraParameters implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final String key, final byte[] data, final String arg1, final String arg2) {
            this.activated = true;
        }

        @Passivation
        public byte[] passivate(final String key, final byte[] data, final String arg1, final String arg2) {
            passivateCalled = true;
            return "passivate".getBytes();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpChangedOrder implements Cloneable {
        public boolean activated = false;
        public boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final byte[] data, final String key) {
            this.activated = true;
        }

        @Passivation
        public void passivate() {
            passivateCalled = true;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpNoActivation implements Cloneable {
        public final boolean activated = false;
        public final boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @MessageType
    public static class Message {
        private final String key;

        public Message(final String key) {
            this.key = key;
        }

        @MessageKey
        public String getKey() {
            return this.key;
        }

    }

    public static class MessageNoTypeInfo {
        private final String key;

        public MessageNoTypeInfo(final String key) {
            this.key = key;
        }

        @MessageKey
        public String getKey() {
            return this.key;
        }

    }

    @MessageType
    public static class InheritMethodTypeAnnotation {}

    @MessageType
    public static class MessgeNoKey extends InheritMethodTypeAnnotation {
        private final String key;

        public MessgeNoKey(final String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    @Mp
    public static class TestMpNoKey implements Cloneable {
        public final boolean activated = false;
        public final boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final MessgeNoKey val) {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @MessageType("Message")
    @Mp
    public static class TestMpMessageTypeClass implements Cloneable {
        @MessageHandler
        public void handleMsg(final MessageNoTypeInfo val) {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpWithReturn implements Cloneable {

        @MessageHandler
        public Message handleMsg(final Message val) {
            return new Message("Yo");
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpWithOnlyBulk implements Cloneable {

        @BulkMessageHandler
        public void handleMsgs(final List<Message> val) {

        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpEvictionNative implements Cloneable {

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Evictable
        public boolean evict() {
            return true;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpEvictionObj implements Cloneable {

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Evictable
        public Boolean evict() {
            return true;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Mp
    public static class TestMpEvictionNoReturn implements Cloneable {

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Evictable
        public void evict() {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

}
