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

package com.nokia.dempsy.cluster.zookeeper;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;

import com.nokia.dempsy.Adaptor;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.output.RelativeOutputSchedule;

@Ignore
public class FullApplication
{
   public int maxValue = 10;
   public AtomicLong finalMessageCount = new AtomicLong(0);
   
   public FullApplication() {}
   
   public static class MyMessage implements Serializable
   {
      private static final long serialVersionUID = 1L;
      
      Integer value;
      
      public MyMessage(int val) { this.value = val; }
      
      @MessageKey
      public Integer getKey() { return value; }
   }
   
   public static class MyMessageCount implements Serializable
   {
      private static final long serialVersionUID = 1L;

      private long count;
      private int value;
      
      public MyMessageCount(long val) { this.count = val; }
      
      @MessageKey
      public Integer getKey() { return 1; }
      
      public int getValue() { return value; }
      
      public long getCount() { return count; }
   }

   public class MyAdaptor implements Adaptor
   {
      private Dispatcher dispatcher;
      private Random random = new Random();
      private AtomicBoolean stop = new AtomicBoolean(false);
      private boolean isStopped = true;
      
      @Override
      public void setDispatcher(Dispatcher dispatcher){ this.dispatcher = dispatcher; }

      @Override
      public void start()
      {
         synchronized(this)
         {
            isStopped = false;
            this.notifyAll();
         }
         
         try
         {
            stop.set(false);
            while (!stop.get())
               dispatcher.dispatch(new MyMessage(random.nextInt(maxValue)));
         }
         finally
         {
            synchronized(this)
            {
               isStopped = true;
               this.notifyAll();
            }
         }
      }

      @Override
      public void stop()
      {
         stop.set(true);
         
         synchronized(this)
         {
            while (!isStopped)
            {
               try { this.wait(); } catch (InterruptedException e) {e.printStackTrace(System.out);}
            }
         }
      }
   }
   
   @MessageProcessor
   public class MyMp implements Cloneable
   {
      public long count;
      public AtomicLong myMpReceived = new AtomicLong(0);
      
      @MessageHandler
      public void handle(MyMessage message)
      {
         count++;
         myMpReceived.incrementAndGet();
      }
      
      @Output
      public MyMessageCount outputMessageCount()
      { 
         return new MyMessageCount(count);
      }
      
      @Override
      public MyMp clone() throws CloneNotSupportedException
      {
         return (MyMp)super.clone();
      }

   }
   
   @MessageProcessor
   public class MyRankMp implements Cloneable
   {
      private long[] count = null;
      
      @MessageHandler
      public void handle(MyMessageCount messageCount)
      {
         count[messageCount.getValue()] = messageCount.getCount();
         finalMessageCount.incrementAndGet();
      }
      
      @Start
      public void init()
      {
         count = new long[maxValue];
      }
      
      @Override
      public MyRankMp clone() throws CloneNotSupportedException
      {
         return (MyRankMp)super.clone();
      }
   }
   
   public ApplicationDefinition getTopology() throws DempsyException
   {
      ApplicationDefinition ret = new ApplicationDefinition(FullApplication.class.getSimpleName()).
            add(new ClusterDefinition(MyAdaptor.class.getSimpleName()).setAdaptor(new MyAdaptor())).
            add(new ClusterDefinition(MyMp.class.getSimpleName()).setMessageProcessorPrototype(new MyMp()).setOutputExecuter(new RelativeOutputSchedule(10, TimeUnit.MICROSECONDS))).
            add(new ClusterDefinition(MyRankMp.class.getSimpleName()).setMessageProcessorPrototype(new MyRankMp()));
      
      return ret;
   }
}