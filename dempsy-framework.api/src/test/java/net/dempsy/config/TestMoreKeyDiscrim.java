package net.dempsy.config;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.Evictable;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Start;

public class TestMoreKeyDiscrim {

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithMessageWithBadKeys() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();

        @MessageType
        class MessageWithBadKeys {
            @MessageKey
            public String key1() {
                return "Hello1";
            }

            @MessageKey("key2")
            public String key2() {
                return "Hello2";
            }
        }

        @Mp
        class mp1 implements Cloneable {
            @MessageHandler("key2")
            public void handle(final MessageWithBadKeys string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Cluster cd1 = new Cluster("test-slot-1");
        cd1.setMessageProcessor(new MessageProcessor<mp1>(new mp1()));
        app.addClusters(cd1);
        app.validate();
    }

}
