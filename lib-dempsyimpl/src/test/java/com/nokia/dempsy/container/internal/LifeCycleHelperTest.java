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

package com.nokia.dempsy.container.internal;

import java.util.Date;

import junit.framework.Assert;

import org.junit.Test;

import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Passivation;

public class LifeCycleHelperTest
{
   @Test
   public void testMethodHandleWithParameters() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMp());
      TestMp mp = (TestMp)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertTrue(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      String ret = new String(helper.passivate(mp));
      Assert.assertTrue(mp.ispassivateCalled());
      Assert.assertEquals("passivate", ret);
   }
   
   @Test
   public void testMethodHandleWithNoParameters() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMpEmptyActivate());
      TestMpEmptyActivate mp = (TestMpEmptyActivate)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertTrue(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      Object ret = helper.passivate(mp);
      Assert.assertTrue(mp.ispassivateCalled());
      Assert.assertNull(ret);
   }

   @Test
   public void testMethodHandleWithOnlyKey() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMpOnlyKey());
      TestMpOnlyKey mp = (TestMpOnlyKey)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertTrue(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      String ret = new String(helper.passivate(mp));
      Assert.assertTrue(mp.ispassivateCalled());
      Assert.assertEquals("passivate", ret);
   }

   @Test
   public void testMethodHandleExtraParameters() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMpExtraParameters());
      TestMpExtraParameters mp = (TestMpExtraParameters)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertTrue(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      String ret = new String(helper.passivate(mp));
      Assert.assertTrue(mp.ispassivateCalled());
      Assert.assertEquals("passivate", ret);
   }

   @Test
   public void testMethodHandleExtraParametersOrderChanged() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMpExtraParametersChangedOrder());
      TestMpExtraParametersChangedOrder mp = (TestMpExtraParametersChangedOrder)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertTrue(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      Object ret = helper.passivate(mp);
      Assert.assertTrue(mp.ispassivateCalled());
      Assert.assertNull(ret);
   }

   @Test
   public void testMethodHandleNoActivation() throws Throwable
   {
      LifecycleHelper helper = new LifecycleHelper(new TestMpNoActivation());
      TestMpNoActivation mp = (TestMpNoActivation)helper.newInstance();
      Assert.assertFalse(mp.isActivated());
      helper.activate(mp, "activate", null);
      Assert.assertFalse(mp.isActivated());
      Assert.assertFalse(mp.ispassivateCalled());
      Object ret = helper.passivate(mp);
      Assert.assertFalse(mp.ispassivateCalled());
      Assert.assertNull(ret);
   }

   @SuppressWarnings("unused")
   @MessageProcessor
   private class TestMp implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Activation
      public void activate(String key, byte[] data){this.activated = true;}
      
      @Passivation
      public byte[] passivate()
      {
         passivateCalled = true;
         return "passivate".getBytes();
      }
      
      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }

   @SuppressWarnings("unused")
   @MessageProcessor
   private class TestMpEmptyActivate implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Activation
      public void activate(){this.activated = true;}
      
      @Passivation
      public void passivate()
      {
         passivateCalled = true;
      }

      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }
   
   @SuppressWarnings("unused")
   @MessageProcessor
   private class TestMpOnlyKey implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Activation
      public void activate(String key){this.activated = true;}
      
      @Passivation
      public byte[] passivate(String key)
      {
         passivateCalled = true;
         return "passivate".getBytes();
      }
      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }

   @SuppressWarnings("unused")
   @MessageProcessor
   private class TestMpExtraParameters implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Activation
      public void activate(String key, byte[] data, String arg1, String arg2){this.activated = true;}

      @Passivation
      public byte[] passivate(String key, byte[] data, String arg1, String arg2)
      {
         passivateCalled = true;
         return "passivate".getBytes();
      }

      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }

   @SuppressWarnings("unused")
   @MessageProcessor
   private class TestMpExtraParametersChangedOrder implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Activation
      public void activate(byte[] data, Integer arg1, String key, Date arg2){this.activated = true;}
      
      @Passivation
      public void passivate(String key, byte[] data, String arg1, String arg2)
      {
         passivateCalled = true;
      }

      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }

   @SuppressWarnings(value="unused")
   @MessageProcessor
   private class TestMpNoActivation implements Cloneable
   {
      private boolean activated = false;
      private boolean passivateCalled = false;
      
      @MessageHandler
      public void handleMsg(Message val){}
      
      @Override
      public Object clone() throws CloneNotSupportedException{return super.clone();}
      
      public boolean isActivated(){ return this.activated;}
      public boolean ispassivateCalled(){ return this.passivateCalled;}
   }

   @SuppressWarnings("unused")
   private class Message
   {
      private String key;
      public Message(String key){ this.key = key;}
      
      @MessageKey
      public String getKey(){return this.key;}
      
   }

}
