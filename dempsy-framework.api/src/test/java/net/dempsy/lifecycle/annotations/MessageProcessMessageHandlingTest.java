package net.dempsy.lifecycle.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.lifecycle.annotations.TestMps.Message;
import net.dempsy.lifecycle.annotations.TestMps.TestMp;
import net.dempsy.lifecycle.annotations.TestMps.TestMpMessageTypeClass;
import net.dempsy.lifecycle.annotations.TestMps.TestMpWithReturn;
import net.dempsy.messages.KeyedMessageWithType;

public class MessageProcessMessageHandlingTest {

    private static KeyedMessageWithType km(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return new KeyExtractor().extract(message).get(0);
    }

    @Test
    public void testMessageTypeResolution() throws Exception {
        final MessageProcessor<TestMp> helper = new MessageProcessor<TestMp>(new TestMp());
        helper.validate();

        final Message m = new Message("yo");
        final TestMp instance = helper.newInstance();
        helper.invoke(instance, km(m));
    }

    @Test
    public void testMpMessageTypeInfo() throws Exception {
        assertThrows(IllegalStateException.class, () -> new MessageProcessor<TestMpMessageTypeClass>(new TestMpMessageTypeClass()));
    }

    @Test
    public void testMpMessageTypeInfoOnReturn() throws Exception {
        final MessageProcessor<TestMpWithReturn> helper = new MessageProcessor<TestMpWithReturn>(new TestMpWithReturn());
        helper.validate();

        final Message m = new Message("yo");
        final TestMpWithReturn instance = helper.newInstance();
        final List<KeyedMessageWithType> kms = helper.invoke(instance, km(m));
        assertEquals(1, kms.size());
        assertNotNull(kms.get(0).messageTypes);
        assertEquals(1, kms.get(0).messageTypes.length);
    }

}
