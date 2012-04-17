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

package com.nokia.dempsy.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Passivation;
import com.nokia.dempsy.container.internal.LifecycleHelper;

/**
 * Formerly there were tests that checked the invocations via the Command
  pattern but as the Command pattern has been removed, so have the tests.
 * 
 */

public class TestInvocation
{
//----------------------------------------------------------------------------
//  Test classes -- must be static/public for introspection
//----------------------------------------------------------------------------

   @MessageProcessor
   public static class InvocationTestMP
   implements Cloneable
   {
      public boolean isActivated;
      public String activationValue;
      public boolean isPassivated;
      public String lastStringHandlerValue;
      public Number lastNumberHandlerValue;
      public boolean outputCalled;

      @Override
      public InvocationTestMP clone()
      throws CloneNotSupportedException
      {
         return (InvocationTestMP)super.clone();
      }

      @Activation
      public void activate(byte[] data)
      {
         isActivated = true;
         activationValue = new String(data);
      }

      @Passivation
      public byte[] passivate()
      {
         isPassivated = true;
         return activationValue.getBytes();
      }

      @MessageHandler
      public int handle(String value)
      {
         lastStringHandlerValue = value;
         return 42;
      }

      @MessageHandler
      public void handle(Number value)
      {
         lastNumberHandlerValue = value;
      }

      @Output
      public String output()
      {
         outputCalled = true;
         return "42";
      }
   }


   public static class InvalidMP_NoAnnotation
   implements Cloneable
   {
      @Override
      public InvocationTestMP clone()
      throws CloneNotSupportedException
      {
         return (InvocationTestMP)super.clone();
      }
   }


   @MessageProcessor
   public static class InvalidMP_NoClone
   {
      // nothing here
   }


   @MessageProcessor
   public static class LifecycleEqualityTestMP
   extends InvocationTestMP
   {
      // most methods are inherited, but clone() has to be declared

      @Override
      public LifecycleEqualityTestMP clone()
      throws CloneNotSupportedException
      {
         return (LifecycleEqualityTestMP)super.clone();
      }
   }


//----------------------------------------------------------------------------
//  Test Cases
//----------------------------------------------------------------------------

   @Test
   public void testLifecycleHelperEqualityAndHashcodeDelegateToMP()
   throws Exception
   {
      LifecycleHelper helper1a = new LifecycleHelper(new InvocationTestMP());
      LifecycleHelper helper1b = new LifecycleHelper(new InvocationTestMP());
      LifecycleHelper helper2  = new LifecycleHelper(new LifecycleEqualityTestMP());

      assertTrue("same MP class means euqal helpers",           helper1a.equals(helper1b));
      assertFalse("different MP class means not-equal helpers", helper1a.equals(helper2));

      assertTrue("same hashcode for same MP class",                     helper1a.hashCode() == helper1b.hashCode());
      assertFalse("different hashcode for different MP class (I hope)", helper1a.hashCode() == helper2.hashCode());
   }


   @Test
   public void testLifeCycleMethods()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);

      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();
      assertNotNull("instantiation failed; null instance", instance);
      assertNotSame("instantiation failed; returned prototype", prototype, instance);

      assertFalse("instance activated before activation method called", instance.isActivated);
      invoker.activate(instance, "ABC".getBytes());
      assertTrue("instance was not activated", instance.isActivated);
      assertEquals("ABC", instance.activationValue);

      assertFalse("instance passivated before passivation method called", instance.isPassivated);
      byte[] data = invoker.passivate(instance);
      assertTrue("instance was not passivated", instance.isPassivated);
      assertEquals("ABC", new String(data));
   }


   @Test(expected=ContainerException.class)
   public void testConstructorFailsIfNotAnnotedAsMP()
   throws Exception
   {
      new LifecycleHelper(new InvalidMP_NoAnnotation());
   }


   @Test(expected=ContainerException.class)
   public void testConstructorFailsIfNoCloneMethod()
   throws Exception
   {
      new LifecycleHelper(new InvalidMP_NoClone());
   }


   @Test
   public void testIsMessageSupported()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);

      assertTrue(invoker.isMessageSupported("foo"));
      assertTrue(invoker.isMessageSupported(new Integer(1)));
      assertTrue(invoker.isMessageSupported(new Double(1.5)));

      assertFalse(invoker.isMessageSupported(new Object()));
      assertFalse(invoker.isMessageSupported(new StringBuilder("foo")));
   }


   @Test
   public void testInvocationExactClass()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);
      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();

      // pre-condition assertion
      assertNull(prototype.lastStringHandlerValue);
      assertNull(instance.lastStringHandlerValue);

      String message = "foo";
      Object o = invoker.invoke(instance, message);
      assertEquals(new Integer(42), o);

      // we assert that the prototype is still null to check for bad code
      assertNull(prototype.lastStringHandlerValue);
      assertEquals(message, instance.lastStringHandlerValue);
   }


   @Test
   public void testInvocationCommonSuperclass()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);
      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();

      Integer message1 = new Integer(1);
      Object o = invoker.invoke(instance, message1);
      assertEquals(message1, instance.lastNumberHandlerValue);
      assertNull(o);

      Double message2 = new Double(1.5);
      invoker.invoke(instance, message2);
      assertEquals(message2, instance.lastNumberHandlerValue);
   }


   @Test(expected=ContainerException.class)
   public void testInvocationFailureNoHandler()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);
      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();

      invoker.invoke(instance, new Object());
   }


   @Test(expected=NullPointerException.class)
   public void testInvocationFailureNullMessage()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);
      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();

      invoker.invoke(instance, null);
   }


   @Test
   public void testOutput()
   throws Exception
   {
      InvocationTestMP prototype = new InvocationTestMP();
      LifecycleHelper invoker = new LifecycleHelper(prototype);
      InvocationTestMP instance = (InvocationTestMP)invoker.newInstance();

      assertFalse("instance says it did output before method called", instance.outputCalled);
      invoker.invokeOutput(instance);
      assertTrue("output method was not called", instance.outputCalled);
   }

}
