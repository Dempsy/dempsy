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

package net.dempsy.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.annotations.MessageHandler;
import net.dempsy.annotations.MessageKey;
import net.dempsy.annotations.MessageProcessor;
import net.dempsy.config.ApplicationDefinition;

public class TestRouterInstantiation {
    @Test
    public void testGetMessages() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final Router router = new Router(app);

        final List<Object> messages = new ArrayList<Object>();
        final Object first = new Object();
        router.getMessages(first, messages);
        assertEquals(1, messages.size());
        assertSame(first, messages.get(0));
    }

    public static class MessageThatFailsOnKeyRetrieve {
        public boolean threw = false;

        @MessageKey
        public String getKey() {
            threw = true;
            throw new RuntimeException("Forced Failure");
        }

    }

    @Test
    public void testDispatchBadMessage() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final Router router = new Router(app);

        Object o;
        router.dispatch(o = new Object() {
            @MessageKey
            public String getKey() {
                return "hello";
            }
        });

        assertTrue(router.stopTryingToSendTheseTypes.contains(o.getClass()));

        final MessageThatFailsOnKeyRetrieve message = new MessageThatFailsOnKeyRetrieve();
        router.dispatch(message);
        assertTrue(message.threw);

        router.dispatch(null); // this should just warn
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullApplicationDef() throws Throwable {
        new Router(null);
    }

    @Test
    public void testGetMessagesNester() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final Router router = new Router(app);

        final List<Object> messages = new ArrayList<Object>();
        final List<Object> parent = new ArrayList<Object>();
        final List<Object> nested = new ArrayList<Object>();
        final Object first = new Object();
        nested.add(first);
        parent.add(nested);
        router.getMessages(parent, messages);
        assertEquals(1, messages.size());
        assertSame(first, messages.get(0));
    }

    @Test
    public void testSpringConfig() throws Throwable {
        try (final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("RouterConfigTest.xml");) {
            ctx.registerShutdownHook();
        }
    }

    @MessageProcessor
    public static class TestMp implements Cloneable {
        @MessageHandler
        public void handle(final String stringMe) {}

        @Override
        public TestMp clone() throws CloneNotSupportedException {
            return (TestMp) super.clone();
        }
    }
}
