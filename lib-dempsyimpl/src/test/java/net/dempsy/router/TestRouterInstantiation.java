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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import net.dempsy.annotations.MessageHandler;
import net.dempsy.annotations.MessageKey;
import net.dempsy.annotations.MessageProcessor;
import net.dempsy.config.ApplicationDefinition;
import net.dempsy.router.Router;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestRouterInstantiation
{
   @Test
   public void testGetMessages() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      Router router = new Router(app);

      List<Object> messages = new ArrayList<Object>();
      Object first = new Object();
      router.getMessages(first, messages);
      Assert.assertEquals(1, messages.size());
      Assert.assertSame(first, messages.get(0));
   }
   
   public static class MessageThatFailsOnKeyRetrieve
   {
      public boolean threw = false;
      @MessageKey
      public String getKey() { threw = true; throw new RuntimeException("Forced Failure"); }
      
   }
   
   @Test
   public void testDispatchBadMessage() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      Router router = new Router(app);

      Object o;
      router.dispatch(o = new Object() {
         @MessageKey
         public String getKey() { return "hello"; }
      });
      
      assertTrue(router.stopTryingToSendTheseTypes.contains(o.getClass()));

      MessageThatFailsOnKeyRetrieve message = new MessageThatFailsOnKeyRetrieve();
      router.dispatch(message);
      assertTrue(message.threw);
      
      router.dispatch(null); // this should just warn
   }
   
   @Test(expected=IllegalArgumentException.class)
   public void testNullApplicationDef() throws Throwable
   {
      new Router(null);
   }

   @Test
   public void testGetMessagesNester() throws Throwable
   {
      ApplicationDefinition app = new ApplicationDefinition("test");
      Router router = new Router(app);
      
      List<Object> messages = new ArrayList<Object>();
      List<Object> parent = new ArrayList<Object>();
      List<Object> nested = new ArrayList<Object>();
      Object first = new Object();
      nested.add(first);
      parent.add(nested);
      router.getMessages(parent, messages);
      Assert.assertEquals(1, messages.size());
      Assert.assertSame(first, messages.get(0));
   }
   
   @Test
   public void testSpringConfig() throws Throwable
   {
         ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("RouterConfigTest.xml");
         ctx.registerShutdownHook();
   }
   
   @MessageProcessor
   public static class TestMp implements Cloneable
   {
      @MessageHandler
      public void handle(String stringMe) {}
      
      public TestMp clone() throws CloneNotSupportedException { return (TestMp)super.clone(); }
   }
}
