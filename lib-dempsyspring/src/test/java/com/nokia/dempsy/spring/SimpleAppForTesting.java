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

package com.nokia.dempsy.spring;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;

public class SimpleAppForTesting
{
   public static AtomicReference<DempsyGrabber> grabber = new AtomicReference<DempsyGrabber>();

   public static class TestMessage
   {
      public String key;
      public TestMessage(String key) { this.key = key; }
      @MessageKey
      public String getKey() { return key; }
   }
   
   @MessageProcessor
   public static class TestMp implements Cloneable
   {
      public TestMessage last = null;
      
      @MessageHandler
      public void handle(TestMessage val) 
      {
         last = val;
      }
      
      public TestMp clone() throws CloneNotSupportedException { return (TestMp)super.clone(); }
   }
   
   public static class TestAdaptor implements Adaptor
   {
      volatile boolean startCalled = false;
      volatile Dispatcher dispatcher = null;
      
      @Override
      public void setDispatcher(Dispatcher dispatcher) {  this.dispatcher = dispatcher; }

      @Override
      public void start()  { startCalled = true; }
      
      @Override
      public void stop() { }
      
   }
   
   public static class DempsyGrabber implements ApplicationContextAware
   {
      public AtomicReference<Dempsy> dempsy = new AtomicReference<Dempsy>();
      public AtomicReference<ApplicationContext> ctx = new AtomicReference<ApplicationContext>();
      
      public DempsyGrabber() { grabber.set(this); }
      
      public boolean waitForDempsy(long millisToWait) throws InterruptedException
      {
         // wait for Dempsy to be initialized
         for (long endTime = System.currentTimeMillis() + millisToWait; endTime > System.currentTimeMillis() && dempsy.get() == null;) Thread.sleep(1);
         return dempsy.get() != null;
      }
      
      public boolean waitForContext(long millisToWait) throws InterruptedException
      {
         // wait for Dempsy to be initialized
         for (long endTime = System.currentTimeMillis() + millisToWait; endTime > System.currentTimeMillis() && ctx.get() == null;) Thread.sleep(1);
         return ctx.get() != null;
      }
      
      public void setDempsy(Dempsy demp)
      {
         dempsy.set(demp);
      }

      @Override
      public void setApplicationContext(ApplicationContext arg0) throws BeansException
      {
         ctx.set(arg0);
      }
   }
}
