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

package net.dempsy.container.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

import net.dempsy.annotations.Activation;
import net.dempsy.annotations.MessageHandler;
import net.dempsy.annotations.MessageKey;
import net.dempsy.annotations.MessageProcessor;
import net.dempsy.annotations.Passivation;

public class LifeCycleHelperTest {
    @Test
    public void testMethodHandleWithParameters() throws Throwable {
        final LifecycleHelper helper = new LifecycleHelper(new TestMp());
        final TestMp mp = (TestMp) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpEmptyActivate());
        final TestMpEmptyActivate mp = (TestMpEmptyActivate) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpOnlyKey());
        final TestMpOnlyKey mp = (TestMpOnlyKey) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpExtraParameters());
        final TestMpExtraParameters mp = (TestMpExtraParameters) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpExtraParametersChangedOrder());
        final TestMpExtraParametersChangedOrder mp = (TestMpExtraParametersChangedOrder) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpNoActivation());
        final TestMpNoActivation mp = (TestMpNoActivation) helper.newInstance();
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
        final LifecycleHelper helper = new LifecycleHelper(new TestMpNoKey());
        final TestMpNoKey mp = (TestMpNoKey) helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate", null);
        assertFalse(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        final Object ret = helper.passivate(mp);
        assertFalse(mp.ispassivateCalled());
        assertNull(ret);
    }

    @MessageProcessor
    private class TestMp implements Cloneable {
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

    @MessageProcessor
    private class TestMpEmptyActivate implements Cloneable {
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

    @MessageProcessor
    private class TestMpOnlyKey implements Cloneable {
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

    @MessageProcessor
    private class TestMpExtraParameters implements Cloneable {
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

    @MessageProcessor
    private class TestMpExtraParametersChangedOrder implements Cloneable {
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

    @MessageProcessor
    private class TestMpNoActivation implements Cloneable {
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

    @SuppressWarnings("unused")
    private class Message {
        private final String key;

        public Message(final String key) {
            this.key = key;
        }

        @MessageKey
        public String getKey() {
            return this.key;
        }

    }

    @SuppressWarnings("unused")
    private class MessgeNoKey {
        private final String key;

        public MessgeNoKey(final String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }

    }

    @MessageProcessor
    private class TestMpNoKey implements Cloneable {
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
}
