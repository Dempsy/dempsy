/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.lifecycle.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.Passivation;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;

/**
 * Formerly there were tests that checked the invocations via the Command
  pattern but as the Command pattern has been removed, so have the tests.
 * 
 */

public class TestInvocation {
    // ----------------------------------------------------------------------------
    // Test classes -- must be static/public for introspection
    // ----------------------------------------------------------------------------

    @MessageType
    public static interface MsgNum {
        Number getNumber();
    }

    public static class MsgInt implements MsgNum {
        public final int value;

        public MsgInt(final int value) {
            this.value = value;
        }

        @MessageKey
        public int getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value == ((MsgInt) o).value;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(value);
        }

        @Override
        public Number getNumber() {
            return new Integer(value);
        }
    }

    public static class MsgDoub implements MsgNum {
        public final double value;

        public MsgDoub(final double value) {
            this.value = value;
        }

        @MessageKey
        public double getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value == ((MsgDoub) o).value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }

        @Override
        public Number getNumber() {
            return new Double(value);
        }
    }

    @MessageType
    public static class MsgString {
        public final String value;

        public MsgString(final String value) {
            this.value = value;
        }

        @MessageKey
        public String getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value.equals(((MsgString) o).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    @Mp
    public static class InvocationTestMp implements Cloneable {
        public boolean isActivated;
        public boolean isPassivated;
        public String lastStringHandlerValue;
        public Number lastNumberHandlerValue;
        public boolean outputCalled;

        @Override
        public InvocationTestMp clone() throws CloneNotSupportedException {
            return (InvocationTestMp) super.clone();
        }

        @Activation
        public void activate() {
            isActivated = true;
        }

        @Passivation
        public void passivate() {
            isPassivated = true;
        }

        @MessageHandler
        public MsgInt handle(final MsgString value) {
            lastStringHandlerValue = value.value;
            return new MsgInt(42);
        }

        @MessageHandler
        public void handle(final MsgNum value) {
            lastNumberHandlerValue = value.getNumber();
        }

        @Output
        public String output() {
            outputCalled = true;
            return "42";
        }
    }

    @MessageType
    public static class MsgNoHandler {

        @MessageKey
        public String key() {
            return "yo";
        }

    }

    public static class InvalidMP_NoAnnotation
            implements Cloneable {
        @Override
        public InvocationTestMp clone()
                throws CloneNotSupportedException {
            return (InvocationTestMp) super.clone();
        }
    }

    @Mp
    public static class InvalidMP_NoClone {
        // nothing here
    }

    @Mp
    public static class LifecycleEqualityTestMP
            extends InvocationTestMp {
        // most methods are inherited, but clone() has to be declared

        @Override
        public LifecycleEqualityTestMP clone()
                throws CloneNotSupportedException {
            return (LifecycleEqualityTestMP) super.clone();
        }
    }

    private static KeyedMessageWithType km(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return new KeyExtractor().extract(message).get(0);
    }

    // ----------------------------------------------------------------------------
    // Test Cases
    // ----------------------------------------------------------------------------

    @Test
    public void testLifecycleHelperEqualityAndHashcodeDelegateToMP() {
        final MessageProcessorLifecycle<InvocationTestMp> helper1a = new MessageProcessor<>(new InvocationTestMp());
        final MessageProcessorLifecycle<InvocationTestMp> helper1b = new MessageProcessor<>(new InvocationTestMp());
        final MessageProcessorLifecycle<LifecycleEqualityTestMP> helper2 = new MessageProcessor<>(new LifecycleEqualityTestMP());

        assertTrue("same MP class means euqal helpers", helper1a.equals(helper1b));
        assertFalse("different MP class means not-equal helpers", helper1a.equals(helper2));

        assertTrue("same hashcode for same MP class", helper1a.hashCode() == helper1b.hashCode());
        assertFalse("different hashcode for different MP class (I hope)", helper1a.hashCode() == helper2.hashCode());
    }

    @Test
    public void testLifeCycleMethods() {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessorLifecycle<InvocationTestMp> invoker = new MessageProcessor<>(prototype);

        final InvocationTestMp instance = invoker.newInstance();
        assertNotNull("instantiation failed; null instance", instance);
        assertNotSame("instantiation failed; returned prototype", prototype, instance);

        assertFalse("instance activated before activation method called", instance.isActivated);
        invoker.activate(instance, null);
        assertTrue("instance was not activated", instance.isActivated);

        assertFalse("instance passivated before passivation method called", instance.isPassivated);
        invoker.passivate(instance);
        assertTrue("instance was not passivated", instance.isPassivated);
    }

    @Test(expected = IllegalStateException.class)
    public void testConstructorFailsIfNoCloneMethod() throws Exception {
        new MessageProcessor<InvalidMP_NoClone>(new InvalidMP_NoClone());
    }

    @Test(expected = IllegalStateException.class)
    public void testConstructorFailsIfNotAnnotedAsMP() throws Exception {
        new MessageProcessor<InvalidMP_NoAnnotation>(new InvalidMP_NoAnnotation());
    }

    @Test
    public void testIsMessageSupported() throws Exception {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);

        assertTrue(invoker.isMessageSupported(new MsgString("foo")));
        assertTrue(invoker.isMessageSupported(new MsgInt(1)));

        assertFalse(invoker.isMessageSupported(new Object()));
        assertFalse(invoker.isMessageSupported(new StringBuilder("foo")));
    }

    @Test
    public void testInvocationExactClass() throws Exception {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);
        final InvocationTestMp instance = invoker.newInstance();

        // pre-condition assertion
        assertNull(prototype.lastStringHandlerValue);
        assertNull(instance.lastStringHandlerValue);

        final MsgString message = new MsgString("foo");
        final Object o = invoker.invoke(instance, km(message)).get(0).message;
        assertEquals(new MsgInt(42), o);

        // we assert that the prototype is still null to check for bad code
        assertNull(prototype.lastStringHandlerValue);
        assertEquals(message.value, instance.lastStringHandlerValue);
    }

    @Test
    public void testInvocationCommonSuperclass() throws Exception {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);
        final InvocationTestMp instance = invoker.newInstance();

        final MsgInt message1 = new MsgInt(1);
        invoker.invoke(instance, km(message1));
        assertEquals(message1.value, instance.lastNumberHandlerValue);

        final MsgDoub message2 = new MsgDoub(1.5);
        invoker.invoke(instance, km(message2));
        assertEquals(message2.value, instance.lastNumberHandlerValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvocationFailureNoHandler() throws Exception {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);
        final InvocationTestMp instance = invoker.newInstance();

        invoker.invoke(instance, km(new MsgNoHandler()));
    }

    @Test(expected = NullPointerException.class)
    public void testInvocationFailureNullMessage() {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);
        final InvocationTestMp instance = invoker.newInstance();

        invoker.invoke(instance, null);
    }

    @Test
    public void testOutput() {
        final InvocationTestMp prototype = new InvocationTestMp();
        final MessageProcessor<InvocationTestMp> invoker = new MessageProcessor<>(prototype);
        final InvocationTestMp instance = invoker.newInstance();

        assertFalse("instance says it did output before method called", instance.outputCalled);
        invoker.invokeOutput(instance);
        assertTrue("output method was not called", instance.outputCalled);
    }

}
