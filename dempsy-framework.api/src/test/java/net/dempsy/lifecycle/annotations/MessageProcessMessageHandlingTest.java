package net.dempsy.lifecycle.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.lifecycle.annotations.TestMps.Message;
import net.dempsy.lifecycle.annotations.TestMps.TestMp;
import net.dempsy.lifecycle.annotations.TestMps.TestMpMessageTypeClass;
import net.dempsy.lifecycle.annotations.TestMps.TestMpWithMultiLevelMessageTypeParameter;
import net.dempsy.lifecycle.annotations.TestMps.TestMpWithReturn;
import net.dempsy.messages.KeyedMessageWithType;

public class MessageProcessMessageHandlingTest {
    @Rule public ExpectedException exception = ExpectedException.none();

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
        exception.expect(IllegalStateException.class);
        new MessageProcessor<TestMpMessageTypeClass>(new TestMpMessageTypeClass());
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
        assertEquals(5, kms.get(0).messageTypes.length);
    }

    @Test
    public void testMpMessageTypeInfoOnParameter() throws Exception {
        final MessageProcessor<TestMpWithMultiLevelMessageTypeParameter> helper = new MessageProcessor<TestMpWithMultiLevelMessageTypeParameter>(
            new TestMpWithMultiLevelMessageTypeParameter());
        helper.validate();

        final Set<String> mts = helper.messagesTypesHandled();
        assertEquals(1, mts.size());

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("no handler for messages of type");

        final Message m = new Message("yo");
        final TestMpWithMultiLevelMessageTypeParameter instance = helper.newInstance();
        helper.invoke(instance, km(m));

    }

}
