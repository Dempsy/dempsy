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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer;

public class TestRunNode
{
   private static Logger logger = LoggerFactory.getLogger(TestRunNode.class);
   
   // internal test state
   volatile boolean failed = false;
   volatile CountDownLatch finished = new CountDownLatch(0);
   
   @Before
   public void setup()
   {
      System.setProperty(RunNode.appdefParam, "TestDempsyApplication.xml");
      System.setProperty(RunNode.applicationParam, "test-app:test-cluster");
      System.setProperty(RunNode.zk_connectParam, "127.0.0.1:2081");

      // reset the grabber
      SimpleAppForTesting.grabber.set(null);
      finished = new CountDownLatch(0);
      failed = false;
   }
   
   public static interface Checker { public void check() throws Throwable; }
   
   public void withAndWithoutZookeeper(Checker toCheck) throws Throwable
   {
      logger.info("********************************************************");
      logger.info("Running " + toCheck + " without zookeeper.");
      // without zookeeper
      toCheck.check();
      
      logger.info("********************************************************");
      logger.info("Running " + toCheck + " with zookeeper.");
      int port = ZookeeperTestServer.findNextPort();
      
      // rerun the setup since the cluster param is overwritten - probably should fix that.
      setup();
      
      // reset the port to the latest.
      System.setProperty(RunNode.zk_connectParam, "127.0.0.1:" + port);
      
      // now run zookeeper
      ZookeeperTestServer server = new ZookeeperTestServer();
      
      try
      {
         server.start();
         
         toCheck.check();
      }
      finally
      {
         server.shutdown();
      }
   }
   
   public Thread makeMainThread()
   {
      return new Thread(new Runnable() { 
         @Override public void run() { 
            try
            {
               RunNode.run(new String[0]);
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

   }
   
   @Test
   public void testNormalStartup() throws Throwable
   {
      withAndWithoutZookeeper(new Checker()
      {
         public void check() throws Throwable
         {
            finished = new CountDownLatch(1); // need to wait on the clean shutdown

            // call main in another thread
            Thread t = makeMainThread();
            t.start();

            // wait for DempsyGrabber
            for (long endTime = System.currentTimeMillis() + 60000; endTime > System.currentTimeMillis() && SimpleAppForTesting.grabber.get() == null;) Thread.sleep(1);
            assertNotNull(SimpleAppForTesting.grabber.get());

            assertTrue(SimpleAppForTesting.grabber.get().waitForDempsy(60000));

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
         
         public String toString() { return "testNormalStartup"; }
      });
   }
   
   @Test
   public void testNormalStartupTestApp1() throws Throwable
   {
      System.setProperty(RunNode.applicationParam, "testApp1:test-cluster");
      System.clearProperty(RunNode.appdefParam);
      testNormalStartup();
   }
   
   @Test(expected=FileNotFoundException.class)
   public void testNoAppGiven() throws Throwable
   {
      System.clearProperty(RunNode.appdefParam);
      try
      {
         RunNode.run(new String[0]);
      }
      catch(Exception e)
      {
         throw e.getCause();
      }
   }
   
   @Test(expected=DempsyException.class)
   public void testNoClusterGiven() throws Throwable
   {
      System.clearProperty(RunNode.applicationParam);
      RunNode.run(new String[0]);
   }
   
   @Test(expected=DempsyException.class)
   public void testNoZkConnectGiven() throws Throwable
   {
      System.clearProperty(RunNode.zk_connectParam);
      RunNode.run(new String[0]);
   }

   @Test(expected=FileNotFoundException.class)
   public void testInvalidAppCtxGiven() throws Throwable
   {
      System.setProperty(RunNode.appdefParam,"IDontExist.xml");
      try
      {
         RunNode.run(new String[0]);
      }
      catch (Throwable th)
      {
         throw th.getCause();
      }
   }
}
