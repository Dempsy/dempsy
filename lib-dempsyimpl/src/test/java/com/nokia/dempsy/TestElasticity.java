package com.nokia.dempsy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.esotericsoftware.kryo.Kryo;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.container.Container;
import com.nokia.dempsy.container.ContainerTestAccess;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.tcp.TcpReceiver;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;
import com.nokia.dempsy.util.Pair;

public class TestElasticity extends DempsyTestBase
{
   static { logger = LoggerFactory.getLogger(TestElasticity.class);  }
   
   private static final int profilerTestNumberCount = 100000;
   
   public static final String actxPath = "testElasticity/NumberCountActx.xml";
   
   //========================================================================
   // Test classes we will be working with. The old word count example.
   //========================================================================
   public static class Number implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private Integer number;
      private int rankIndex;

      public Number() {} // needed for kryo-serializer
      public Number(Integer number, int rankIndex) { this.number = number; this.rankIndex = rankIndex; }
      
      @MessageKey
      public Integer getNumber() { return number; }
      
      @Override
      public String toString() { return "" + number; }
   }
   
   public static class NumberCount implements Serializable
   {
      private static final long serialVersionUID = 1L;
      public Integer number;
      public long count;
      public int rankIndex;
      
      public NumberCount(Number number, long count) { this.number = number.getNumber(); this.count = count; this.rankIndex = number.rankIndex; }
      public NumberCount() {} // for kryo
      
      static final Integer one = new Integer(1);
      
      @MessageKey
      public Integer getKey() { return number; } 
      
      @Override public String toString() { return "(" + count + " "  + number + "s)"; }
   }
   
   public static class NumberProducer implements Adaptor
   {
      public Dispatcher dispatcher = null;
      @Override
      public void setDispatcher(Dispatcher dispatcher) { this.dispatcher = dispatcher; }

      @Override
      public void start() { }
      
      @Override
      public void stop() { }
   }
   
   @MessageProcessor
   public static class NumberCounter implements Cloneable
   {
      public static AtomicLong messageCount = new AtomicLong(0);
      long counter = 0;
      String wordText;
      
      @Activation
      public void initMe(String key) { this.wordText = key; }
      
      @MessageHandler
      public NumberCount handle(Number word) { messageCount.incrementAndGet(); return new NumberCount(word,counter++); }
      
      @Override
      public NumberCounter clone() throws CloneNotSupportedException { return (NumberCounter) super.clone(); }
   }

   @MessageProcessor
   public static class NumberRank implements Cloneable
   {
      public final AtomicLong totalMessages = new AtomicLong(0);
      
      @SuppressWarnings({"unchecked","rawtypes"})
      final public AtomicReference<Map<Integer,Long>[]> countMap = new AtomicReference(new Map[1000]);
      
      {
         for (int i = 0; i < 1000; i++)
            countMap.get()[i] = new ConcurrentHashMap<Integer, Long>();
      }
      
      @MessageHandler
      public void handle(NumberCount wordCount) 
      {
         totalMessages.incrementAndGet();
         countMap.get()[wordCount.rankIndex].put(wordCount.number, wordCount.count); 
      }

      @Override
      public NumberRank clone() throws CloneNotSupportedException { return (NumberRank) super.clone(); }
      
      public List<Pair<Integer,Long>> getPairs(int rankIndex)
      {
         List<Pair<Integer,Long>> ret = new ArrayList<Pair<Integer,Long>>(countMap.get()[rankIndex].size() + 10);
         for (Map.Entry<Integer, Long> cur : countMap.get()[rankIndex].entrySet())
            ret.add(new Pair<Integer, Long>(cur.getKey(),cur.getValue()));
         Collections.sort(ret, new Comparator<Pair<Integer,Long>>()
               {
                  @Override
                  public int compare(Pair<Integer, Long> o1, Pair<Integer, Long> o2)
                  {
                     return o2.getSecond().compareTo(o1.getSecond());
                  }
               });
         return ret;
      }
   }
   
   public static class NumberCounterKryoOptimizer implements KryoOptimizer
   {
      public static NumberCounterKryoOptimizer instance = new NumberCounterKryoOptimizer();
      
      @Override
      public void preRegister(Kryo kryo)
      {
         kryo.setRegistrationRequired(true);
         kryo.register(Number.class);
         kryo.register(NumberCount.class);
      }

      @Override
      public void postRegister(Kryo kryo) { }
   }
   //========================================================================
   
   @SuppressWarnings("unchecked")
   @Test
   public void testForProfiler() throws Throwable
   {
      // set up the test.
      final Number[] numbers = new Number[profilerTestNumberCount];
      Random random = new Random();
      for (int i = 0; i < numbers.length; i++)
       numbers[i] = new Number(random.nextInt(1000),0);
      
      runAllCombinations(new Checker()
      {
         @Override
         public void check(ClassPathXmlApplicationContext[] contexts) throws Throwable
         {
            try
            {
               logger.trace("==== Starting ...");
               
               boolean exactCheck = !TestUtils.getReceiver(2, contexts).getFailFast();
               
               // set blocking on all tcp receivers if exactCheck is set
               if (exactCheck)
               {
                  for (int i = 1; i < contexts.length; i++)
                  {
                     Receiver r = TestUtils.getReceiver(i, contexts);
                     if (TcpReceiver.class.isAssignableFrom(r.getClass()))
                        ((TcpReceiver)r).setBlocking(true);
                  }
               }
               
               // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
               NumberRank rank = (NumberRank)TestUtils.getMp(4,contexts);

               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3));
               logger.trace("==== Clusters balanced");

               // grab the adaptor from the 0'th cluster + the 0'th (only) node.
               NumberProducer adaptor = (NumberProducer)TestUtils.getAdaptor(0,contexts);

               // grab access to the Dispatcher from the Adaptor
               final Dispatcher dispatcher = adaptor.dispatcher;
               
               for (int i = 0; i < numbers.length; i++)
                  dispatcher.dispatch(numbers[i]);
               
               if (exactCheck)
               {
                  logger.trace("====> Checking exact count.");
                  
                  // keep going as long as they are trickling in.
                  long lastNumberOfMessages = -1;
                  while (rank.totalMessages.get() > lastNumberOfMessages)
                  {
                     lastNumberOfMessages = rank.totalMessages.get();
                     if (TestUtils.poll(baseTimeoutMillis, rank.totalMessages, new TestUtils.Condition<AtomicLong>()
                     {
                        @Override public boolean conditionMet(AtomicLong o) { return o.get() == profilerTestNumberCount; }
                     }))
                        break;
                  }
                  
                  assertEquals(profilerTestNumberCount,rank.totalMessages.get());
                  assertEquals(profilerTestNumberCount,NumberCounter.messageCount.get());
               }

            }
            finally {}
         }
         
         @Override
         public String toString() { return "testForProfiler"; }
         
         @Override
         public void setup() 
         {
            NumberCounter.messageCount.set(0);
            DempsyTestBase.TestKryoOptimizer.proxy = NumberCounterKryoOptimizer.instance;
         }
      },
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster0"), // adaptor
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), // 3 NumberCounter Mps
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), //  .
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), //  .
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster2"));// NumberRank

   }
   
   static private TestUtils.Condition<Thread> threadStoppedCondition = new TestUtils.Condition<Thread>()
   {
      @Override
      public boolean conditionMet(Thread o) { return !o.isAlive(); }
   };
   
   private void verifyMessagesThrough(final AtomicInteger rankIndexToSend, 
         AtomicBoolean keepGoing, Runnable sendMessages, NumberRank rank,
         ClassPathXmlApplicationContext[] contexts, int indexOfNumberCountMp, 
         Dispatcher dispatcher, int width, ClassPathXmlApplicationContext... additionalContexts) throws Throwable
   {
      rankIndexToSend.incrementAndGet();
      keepGoing.set(true);
      Thread tmpThread = new Thread(sendMessages);
      tmpThread.start();

      // wait for the messages to get all the way through
      assertTrue(TestUtils.poll(baseTimeoutMillis, rank, new TestUtils.Condition<NumberRank>()
      {
         @Override public boolean conditionMet(NumberRank rank) { return rank.countMap.get()[rankIndexToSend.get()].size() == 20; }
      }));
      keepGoing.set(false);

      // wait for the thread to exit.
      assertTrue(TestUtils.poll(baseTimeoutMillis, tmpThread, threadStoppedCondition));

      // This only works when the container blocks and after the output's are set up from the previous
      //  other thread run.
      if (!TestUtils.getReceiver(indexOfNumberCountMp, contexts).getFailFast())
      {
         // now that all of the through details are there, we want to clear the rank map and send a single
         // value per slot back through.
         rankIndexToSend.incrementAndGet();
         for (int num = 0; num < 20; num++)
            dispatcher.dispatch(new Number(num,rankIndexToSend.get()));
         // all 20 should make it to the end.
         assertTrue(TestUtils.poll(baseTimeoutMillis, rank, new TestUtils.Condition<NumberRank>()
         {
            @Override public boolean conditionMet(NumberRank rank) { return rank.countMap.get()[rankIndexToSend.get()].size() == 20; }
         }));
      }
      
      List<ClassPathXmlApplicationContext> tmpl = new ArrayList<ClassPathXmlApplicationContext>(contexts.length + additionalContexts.length);
      tmpl.addAll(Arrays.asList(contexts));
      tmpl.addAll(Arrays.asList(additionalContexts));
      checkMpDistribution("test-cluster1", tmpl.toArray(new ClassPathXmlApplicationContext[0]), 20, width);
   }

   @SuppressWarnings("unchecked")
   @Test
   public void testNumberCountDropOneAndReAdd() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(ClassPathXmlApplicationContext[] contexts) throws Throwable
         {
            // keepGoing is for the separate thread that pumps messages into the system.
            final AtomicBoolean keepGoing = new AtomicBoolean(true);
            try
            {
               logger.trace("==== <- Starting");
               
               // grab the adaptor from the 0'th cluster + the 0'th (only) node.
               NumberProducer adaptor = (NumberProducer)TestUtils.getAdaptor(0,contexts);

               // grab access to the Dispatcher from the Adaptor
               final Dispatcher dispatcher = adaptor.dispatcher;

               // This is a Runnable that will pump messages to the dispatcher until keepGoing is
               //  flipped to 'false.' It's stateless so it can be reused as needed.
               final AtomicInteger rankIndexToSend = new AtomicInteger(0);
               Runnable sendMessages = new Runnable()
               {
                  @Override public void run()
                  {
                     // send a few numbers. There are 20 shards so in order to cover all 
                     // shards we can send in 20 messages. It just so happens that the hashCode
                     // for an integer is the integer itself so we can get every shard by sending
                     while (keepGoing.get())
                        for (int num = 0; num < 20; num++)
                           dispatcher.dispatch(new Number(num,rankIndexToSend.get()));
                  }
               };
               
               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3));
               
               // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
               NumberRank rank = (NumberRank)TestUtils.getMp(4,contexts);

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3);

               // now kill a node.
               Dempsy middleDempsy = TestUtils.getDempsy(2, contexts); // select the middle Dempsy
               logger.trace("==== Stopping middle Dempsy servicing shards " + TestUtils.getNode(2, contexts).strategyInbound);
               middleDempsy.stop();
               assertTrue(middleDempsy.waitToBeStopped(baseTimeoutMillis));
               logger.trace("==== Stopped middle Dempsy");

               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2));

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2);
               
               // now, bring online another instance.
               logger.trace("==== starting a new one");
               ClassPathXmlApplicationContext actx = startAnotherNode(2);
               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3, actx));

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3, actx);
            }
            finally
            {
               keepGoing.set(false);
               logger.trace("==== Exiting test.");
            }
         }
         
         @Override
         public String toString() { return "testNumberCountDropOneAndReAdd"; }
         
         @Override
         public void setup() 
         {
            DempsyTestBase.TestKryoOptimizer.proxy = NumberCounterKryoOptimizer.instance;
         }
      },
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster0"), // adaptor
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), // 3 NumberCounter Mps
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), //  .
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), //  .
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster2"));// NumberRank
   }
   
   @SuppressWarnings("unchecked")
   @Test
   public void testNumberCountAddOneThenDrop() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(ClassPathXmlApplicationContext[] contexts) throws Throwable
         {
            // keepGoing is for the separate thread that pumps messages into the system.
            final AtomicBoolean keepGoing = new AtomicBoolean(true);
            try
            {
               logger.trace("==== <- Starting");
               
               // grab the adaptor from the 0'th cluster + the 0'th (only) node.
               NumberProducer adaptor = (NumberProducer)TestUtils.getAdaptor(0,contexts);

               // grab access to the Dispatcher from the Adaptor
               final Dispatcher dispatcher = adaptor.dispatcher;

               // This is a Runnable that will pump messages to the dispatcher until keepGoing is
               //  flipped to 'false.' It's stateless so it can be reused as needed.
               final AtomicInteger rankIndexToSend = new AtomicInteger(0);
               Runnable sendMessages = new Runnable()
               {
                  @Override public void run()
                  {
                     // send a few numbers. There are 20 shards so in order to cover all 
                     // shards we can send in 20 messages. It just so happens that the hashCode
                     // for an integer is the integer itself so we can get every shard by sending
                     while (keepGoing.get())
                        for (int num = 0; num < 20; num++)
                           dispatcher.dispatch(new Number(num,rankIndexToSend.get()));
                  }
               };
               
               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2));
               
               // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
               NumberRank rank = (NumberRank)TestUtils.getMp(3,contexts);

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2);

               // now, bring online another instance.
               logger.trace("==== starting a new one");
               ClassPathXmlApplicationContext actx = startAnotherNode(2);
               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3, actx));

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3, actx);
               
               // now kill a node.
               Dempsy middleDempsy = TestUtils.getDempsy(2, contexts); // select the middle Dempsy
               logger.trace("==== Stopping middle Dempsy servicing shards " + TestUtils.getNode(2, contexts).strategyInbound);
               middleDempsy.stop();
               assertTrue(middleDempsy.waitToBeStopped(baseTimeoutMillis));
               logger.trace("==== Stopped middle Dempsy");

               assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2, actx));

               verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2, actx);
            }
            finally
            {
               keepGoing.set(false);
               logger.trace("==== Exiting test.");
            }
         }
         
         @Override
         public String toString() { return "testNumberCountDropOneAndReAdd"; }
         
         @Override
         public void setup() 
         {
            DempsyTestBase.TestKryoOptimizer.proxy = NumberCounterKryoOptimizer.instance;
         }
      },
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster0"), // adaptor
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), // 2 NumberCounter Mps
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"), //  .
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster2"));// NumberRank
   }

   /**
    * Verify that the shards are correctly balanced for the given cluster and that the
    * given expectedNumberOfNodes are available.
    */
   private void checkMpDistribution(String cluster, ClassPathXmlApplicationContext[] contexts, int totalNumberOfShards, int expectedNumberOfNodes) throws Throwable
   {
      List<Dempsy.Application.Cluster.Node> nodes = TestUtils.getNodes(contexts, cluster);
      List<Container> containers = new ArrayList<Container>(nodes.size());
      
      for (Dempsy.Application.Cluster.Node node : nodes)
      {
         Dempsy dempsy = node.yspmeDteg();
         if (dempsy.isRunning())
            containers.add(node.getMpContainer());
      }
         
      assertEquals(expectedNumberOfNodes,containers.size());
      
      final int minNumberOfShards = (int)Math.floor((double)totalNumberOfShards/(double)expectedNumberOfNodes);
      final int maxNumberOfShards = (int)Math.ceil((double)totalNumberOfShards/(double)expectedNumberOfNodes);
      
      int totalNum = 0;
      for (Container cur : containers)
      {
         ContainerTestAccess container = (ContainerTestAccess)cur;
         int curNum = container.getProcessorCount();
         assertTrue(""+ curNum + " <= " + maxNumberOfShards,curNum <= maxNumberOfShards); 
         assertTrue(""+ curNum + " >= " + maxNumberOfShards,curNum >= minNumberOfShards);
         totalNum += curNum;
      }
      
      assertEquals(totalNumberOfShards,totalNum);
   }
}
