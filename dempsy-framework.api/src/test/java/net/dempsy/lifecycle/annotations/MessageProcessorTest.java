/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.lifecycle.annotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import net.dempsy.DempsyException;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotations.TestMps.TestMp;
import net.dempsy.lifecycle.annotations.TestMps.TestMpActivateWithMessage;
import net.dempsy.lifecycle.annotations.TestMps.TestMpActivateWithMessageOnly;
import net.dempsy.lifecycle.annotations.TestMps.TestMpChangedOrder;
import net.dempsy.lifecycle.annotations.TestMps.TestMpEmptyActivate;
import net.dempsy.lifecycle.annotations.TestMps.TestMpEvictionNative;
import net.dempsy.lifecycle.annotations.TestMps.TestMpEvictionNoReturn;
import net.dempsy.lifecycle.annotations.TestMps.TestMpEvictionObj;
import net.dempsy.lifecycle.annotations.TestMps.TestMpExtraParameters;
import net.dempsy.lifecycle.annotations.TestMps.TestMpNoActivation;
import net.dempsy.lifecycle.annotations.TestMps.TestMpNoKey;
import net.dempsy.lifecycle.annotations.TestMps.TestMpOnlyKey;

public class MessageProcessorTest {
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testMethodHandleWithParameters() throws Throwable {
        final MessageProcessor<TestMp> helper = new MessageProcessor<TestMp>(new TestMp());
        helper.validate();
        final TestMp mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
    }

    @Test
    public void testMethodHandleWithNoParameters() throws Throwable {
        final MessageProcessor<TestMpEmptyActivate> helper = new MessageProcessor<TestMpEmptyActivate>(new TestMpEmptyActivate());
        helper.validate();
        final TestMpEmptyActivate mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
    }

    @Test
    public void testMethodHandleWithOnlyKey() throws Throwable {
        final MessageProcessor<TestMpOnlyKey> helper = new MessageProcessor<TestMpOnlyKey>(new TestMpOnlyKey());
        helper.validate();
        final TestMpOnlyKey mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
    }

    @Test
    public void testMethodHandleExtraParameters() throws Throwable {
        exception.expect(DempsyException.class);
        final MessageProcessor<TestMpExtraParameters> helper = new MessageProcessor<TestMpExtraParameters>(new TestMpExtraParameters());
        helper.validate();
        final TestMpExtraParameters mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
    }

    @Test
    public void testMethodHandleOrderChanged() throws Throwable {
        final MessageProcessor<TestMpChangedOrder> helper = new MessageProcessor<TestMpChangedOrder>(new TestMpChangedOrder());
        helper.validate();
        final TestMpChangedOrder mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
    }

    @Test
    public void testMethodHandleWithMessage() throws Throwable {
        final MessageProcessor<TestMpActivateWithMessage> helper = new MessageProcessor<TestMpActivateWithMessage>(new TestMpActivateWithMessage());
        helper.validate();
        final TestMpActivateWithMessage mp = helper.newInstance();
        assertFalse(mp.activated);
        final TestMps.Message message = new TestMps.Message("activate");
        helper.activate(mp, message.getKey(), message);
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
        assertSame(message, mp.message);
    }

    @Test
    public void testMethodHandleWithMessageOnly() throws Throwable {
        final MessageProcessor<TestMpActivateWithMessageOnly> helper = new MessageProcessor<TestMpActivateWithMessageOnly>(new TestMpActivateWithMessageOnly());
        helper.validate();
        final TestMpActivateWithMessageOnly mp = helper.newInstance();
        assertFalse(mp.activated);
        final TestMps.Message message = new TestMps.Message("activate");
        helper.activate(mp, message.getKey(), message);
        assertTrue(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertTrue(mp.passivateCalled);
        assertSame(message, mp.message);
    }

    @Test
    public void testMethodHandleNoActivation() throws Throwable {
        final MessageProcessor<TestMpNoActivation> helper = new MessageProcessor<TestMpNoActivation>(new TestMpNoActivation());
        helper.validate();
        final TestMpNoActivation mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertFalse(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertFalse(mp.passivateCalled);
    }

    @Test(expected = IllegalStateException.class)
    public void testMethodHandleNoKey() throws Throwable {
        final MessageProcessor<TestMpNoKey> helper = new MessageProcessor<TestMpNoKey>(new TestMpNoKey());
        final TestMpNoKey mp = helper.newInstance();
        assertFalse(mp.activated);
        helper.activate(mp, "activate", new Object());
        assertFalse(mp.activated);
        assertFalse(mp.passivateCalled);
        helper.passivate(mp);
        assertFalse(mp.passivateCalled);
    }

    @Test
    public void testEvictionNative() {
        final MessageProcessor<TestMpEvictionNative> helper = new MessageProcessor<TestMpEvictionNative>(new TestMpEvictionNative());
        final TestMpEvictionNative mp = helper.newInstance();
        assertTrue(helper.isEvictionSupported());
        assertTrue(helper.invokeEvictable(mp));
    }

    @Test
    public void testEvictionObj() {
        final MessageProcessor<TestMpEvictionObj> helper = new MessageProcessor<TestMpEvictionObj>(new TestMpEvictionObj());
        final TestMpEvictionObj mp = helper.newInstance();
        assertTrue(helper.isEvictionSupported());
        assertTrue(helper.invokeEvictable(mp));
    }

    @Test
    public void testEvictionNoReturn() {
        exception.expect(DempsyException.class);
        new MessageProcessor<TestMpEvictionNoReturn>(new TestMpEvictionNoReturn());
    }
}
