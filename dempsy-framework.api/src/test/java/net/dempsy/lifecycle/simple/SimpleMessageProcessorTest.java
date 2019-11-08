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

package net.dempsy.lifecycle.simple;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.KeyedMessage;

public class SimpleMessageProcessorTest {
    @Test
    public void testMethodHandleWithParameters() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMp());
        final TestMp mp = (TestMp)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleWithNoParameters() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpEmptyActivate());
        final TestMpEmptyActivate mp = (TestMpEmptyActivate)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleWithOnlyKey() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpOnlyKey());
        final TestMpOnlyKey mp = (TestMpOnlyKey)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleExtraParameters() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpExtraParameters());
        final TestMpExtraParameters mp = (TestMpExtraParameters)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleExtraParametersOrderChanged() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpExtraParametersChangedOrder());
        final TestMpExtraParametersChangedOrder mp = (TestMpExtraParametersChangedOrder)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertTrue(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertTrue(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleNoActivation() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpNoActivation());
        final TestMpNoActivation mp = (TestMpNoActivation)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertFalse(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertFalse(mp.ispassivateCalled());
    }

    @Test
    public void testMethodHandleNoKey() throws Throwable {
        final MessageProcessor helper = new MessageProcessor(() -> new TestMpNoKey());
        final TestMpNoKey mp = (TestMpNoKey)helper.newInstance();
        assertFalse(mp.isActivated());
        helper.activate(mp, "activate");
        assertFalse(mp.isActivated());
        assertFalse(mp.ispassivateCalled());
        helper.passivate(mp);
        assertFalse(mp.ispassivateCalled());
    }

    private class TestMp implements Mp {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        @Override
        public void activate(final Object key) {
            this.activated = true;
        }

        @Override
        public void passivate() {
            passivateCalled = true;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    private class TestMpEmptyActivate implements Mp {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        @Override
        public void activate(final Object key) {
            this.activated = true;
        }

        @Override
        public void passivate() {
            passivateCalled = true;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    private class TestMpOnlyKey implements Mp {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        @Override
        public void activate(final Object key) {
            this.activated = true;
        }

        @Override
        public void passivate() {
            passivateCalled = true;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    private class TestMpExtraParameters implements Mp {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        @Override
        public void activate(final Object key) {
            this.activated = true;
        }

        @Override
        public void passivate() {
            passivateCalled = true;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    private class TestMpExtraParametersChangedOrder implements Mp {
        private boolean activated = false;
        private boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        @Override
        public void activate(final Object key) {
            this.activated = true;
        }

        @Override
        public void passivate() {
            passivateCalled = true;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }

    private class TestMpNoActivation implements Mp {
        private final boolean activated = false;
        private final boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
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

    private class TestMpNoKey implements Mp {
        private final boolean activated = false;
        private final boolean passivateCalled = false;

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage val) {
            return null;
        }

        public boolean isActivated() {
            return this.activated;
        }

        public boolean ispassivateCalled() {
            return this.passivateCalled;
        }
    }
}
