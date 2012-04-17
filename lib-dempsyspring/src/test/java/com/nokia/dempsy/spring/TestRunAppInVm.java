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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.DempsyException;

public class TestRunAppInVm
{
   // internal test state
   volatile boolean failed = false;
   volatile CountDownLatch finished = new CountDownLatch(0);
   
   @Before
   public void setup()
   {
      System.setProperty(RunAppInVm.appdefParam, "TestDempsyApplication.xml");
      System.setProperty(RunAppInVm.applicationParam, "test-app");

      // reset the grabber
      SimpleAppForTesting.grabber.set(null);
      finished = new CountDownLatch(0);
      failed = false;
   }
   
   @Test
   public void testNormalStartup() throws Throwable
   {
      finished = new CountDownLatch(1); // need to wait on the clean shutdown

      // call run in another thread
      Thread t = new Thread(new Runnable() { 
         @Override public void run() { 
            try
            {
               RunAppInVm.run(new String[0]);
            }
            catch (Throwable th)
            {
               failed = true;
            }
            finally
            {
               finished.countDown();
            }
         }
      }, "DempsyStartup");

      t.start();

      // wait for DempsyGrabber
      for (long endTime = System.currentTimeMillis() + 60000; endTime > System.currentTimeMillis() && SimpleAppForTesting.grabber.get() == null;) Thread.sleep(1);
      assertNotNull(SimpleAppForTesting.grabber.get());

      assertTrue(SimpleAppForTesting.grabber.get().waitForDempsy(60000));
      assertTrue(SimpleAppForTesting.grabber.get().waitForContext(60000));

      Dempsy dempsy = SimpleAppForTesting.grabber.get().dempsy.get();

      // wait for Dempsy to be running
      for (long endTime = System.currentTimeMillis() + 60000; endTime > System.currentTimeMillis() && !dempsy.isRunning();) Thread.sleep(1);
      assertTrue(dempsy.isRunning());

      Thread.sleep(500); // let the thing run for a bit.

      dempsy.stop();

      // wait for Dempsy to be stopped
      for (long endTime = System.currentTimeMillis() + 60000; endTime > System.currentTimeMillis() && dempsy.isRunning();) Thread.sleep(1);
      assertFalse(dempsy.isRunning());

      assertFalse(failed);
   }
   
   @Test
   public void testNormalStartupTestApp1() throws Throwable
   {
      System.clearProperty(RunAppInVm.appdefParam);
      System.setProperty(RunAppInVm.applicationParam, "testApp1");
      testNormalStartup();
   }

   @Test(expected=DempsyException.class)
   public void testNoAppOrAppDefGiven() throws Throwable
   {
      System.clearProperty(RunAppInVm.appdefParam);
      System.clearProperty(RunAppInVm.applicationParam);
      RunAppInVm.run(new String[0]);
   }
   
   @Test(expected=FileNotFoundException.class)
   public void testNoAppGiven() throws Throwable
   {
      System.clearProperty(RunAppInVm.appdefParam);
      try
      {
         RunAppInVm.run(new String[0]);
      }
      catch(Throwable t)
      {
         throw t.getCause();
      }
   }
   
   @Test(expected=FileNotFoundException.class)
   public void testInvalidAppCtxGiven() throws Throwable
   {
      System.setProperty(RunAppInVm.appdefParam,"IDontExist.xml");
      try
      {
         RunAppInVm.run(new String[0]);
      }
      catch (Throwable th)
      {
         throw th.getCause();
      }
   }
   
}
