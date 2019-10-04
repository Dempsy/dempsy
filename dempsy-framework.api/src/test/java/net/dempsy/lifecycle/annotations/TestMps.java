package net.dempsy.lifecycle.annotations;

import java.util.Date;

import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Passivation;

public class TestMps {
    @Mp
    public static class TestMp implements Cloneable {
        private boolean activated = false;
        private boolean passivateCalled = false;

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

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    @Mp
    public static class TestMpEmptyActivate implements Cloneable {
        private boolean activated = false;
        private boolean passivateCalled = false;

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

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    @Mp
    public static class TestMpOnlyKey implements Cloneable {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final String key) {
            this.activated = true;
        }

        @Passivation
        public byte[] passivate(final String key) {
            passivateCalled = true;
            return "passivate".getBytes();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    @Mp
    public static class TestMpExtraParameters implements Cloneable {
        private boolean activated = false;
        private boolean passivateCalled = false;

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

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    @Mp
    public static class TestMpExtraParametersChangedOrder implements Cloneable {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Activation
        public void activate(final byte[] data, final Integer arg1, final String key, final Date arg2) {
            this.activated = true;
        }

        @Passivation
        public void passivate(final String key, final byte[] data, final String arg1, final String arg2) {
            passivateCalled = true;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    @Mp
    public static class TestMpNoActivation implements Cloneable {
        private final boolean activated = false;
        private final boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final Message val) {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
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
        private final boolean activated = false;
        private final boolean passivateCalled = false;

        @MessageHandler
        public void handleMsg(final MessgeNoKey val) {}

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
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
}
