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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotations.TestMps.TestMp;
import net.dempsy.lifecycle.annotations.TestMps.TestMpEmptyActivate;
import net.dempsy.lifecycle.annotations.TestMps.TestMpExtraParameters;
import net.dempsy.lifecycle.annotations.TestMps.TestMpExtraParametersChangedOrder;
import net.dempsy.lifecycle.annotations.TestMps.TestMpNoActivation;
import net.dempsy.lifecycle.annotations.TestMps.TestMpNoKey;
import net.dempsy.lifecycle.annotations.TestMps.TestMpOnlyKey;

public class MessageProcessorTest {

    @Test
    public void testMethodHandleWithParameters() throws Throwable {
        final MessageProcessor<TestMp> helper = new MessageProcessor<TestMp>(new TestMp());
        helper.validate();
        final TestMp mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final String ret = new String(helper.passivate(mp));
        assertTrue(mp.ispassivateCalled());
        assertEquals("passivate", ret);
    }

    @Test
    public void testMethodHandleWithNoParameters() throws Throwable {
        final MessageProcessor<TestMpEmptyActivate> helper = new MessageProcessor<TestMpEmptyActivate>(new TestMpEmptyActivate());
        helper.validate();
        final TestMpEmptyActivate mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final Object ret = helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
        assertNull(ret);
    }

    @Test
    public void testMethodHandleWithOnlyKey() throws Throwable {
        final MessageProcessor<TestMpOnlyKey> helper = new MessageProcessor<TestMpOnlyKey>(new TestMpOnlyKey());
        helper.validate();
        final TestMpOnlyKey mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final String ret = new String(helper.passivate(mp));
        assertTrue(mp.ispassivateCalled());
        assertEquals("passivate", ret);
    }

    @Test
    public void testMethodHandleExtraParameters() throws Throwable {
        final MessageProcessor<TestMpExtraParameters> helper = new MessageProcessor<TestMpExtraParameters>(new TestMpExtraParameters());
        helper.validate();
        final TestMpExtraParameters mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final String ret = new String(helper.passivate(mp));
        assertTrue(mp.ispassivateCalled());
        assertEquals("passivate", ret);
    }

    @Test
    public void testMethodHandleExtraParametersOrderChanged() throws Throwable {
        final MessageProcessor<TestMpExtraParametersChangedOrder> helper = new MessageProcessor<TestMpExtraParametersChangedOrder>(
                new TestMpExtraParametersChangedOrder());
        helper.validate();
        final TestMpExtraParametersChangedOrder mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final Object ret = helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
        assertNull(ret);
    }

    @Test
    public void testMethodHandleNoActivation() throws Throwable {
        final MessageProcessor<TestMpNoActivation> helper = new MessageProcessor<TestMpNoActivation>(new TestMpNoActivation());
        helper.validate();
        final TestMpNoActivation mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertFalse(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final Object ret = helper.passivate(mp);
        assertFalse(mp.ispassivateCalled());
        assertNull(ret);
    }

    @Test
    public void testMethodHandleNoKey() throws Throwable {
        final MessageProcessor<TestMpNoKey> helper = new MessageProcessor<TestMpNoKey>(new TestMpNoKey());
        final TestMpNoKey mp = helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertFalse(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final Object ret = helper.passivate(mp);
        assertFalse(mp.ispassivateCalled());
        assertNull(ret);
    }

}
