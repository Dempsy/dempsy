package com.nokia.dempsy;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.esotericsoftware.kryo.Kryo;
import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;
import com.nokia.dempsy.util.Pair;

public class TestWordCount extends DempsyTestBase
{
   static { logger = LoggerFactory.getLogger(TestWordCount.class);  }
   
   public static final String actxPath = "testWordCount/WordCountActx.xml";
   public static final String wordResource = "testDempsy/testWordCount/AV1611Bible.txt.gz";
   
   @Before
   public void setup()
   {
      WordProducer.latch = new CountDownLatch(0);
   }
   
   //========================================================================
   // Test classes we will be working with. The old word count example.
   //========================================================================
   public static class Word implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private String word;

      public Word() {} // needed for kryo-serializer
      public Word(String word) { this.word = word; }
      
      @MessageKey
      public String getWord() { return word; }
   }
   
   public static class WordCount implements Serializable
   {
      private static final long serialVersionUID = 1L;
      public String word;
      public long count;
      public WordCount(Word word, long count) { this.word = word.getWord(); this.count = count; }
      public WordCount() {} // for kryo
      
      static final Integer one = new Integer(1);
      
      @MessageKey
      public String getKey() { return word; } 
   }
   
   public static class WordProducer implements Adaptor
   {
      private final AtomicBoolean isRunning = new AtomicBoolean(false);
      private Dispatcher dispatcher = null;
      private AtomicBoolean done = new AtomicBoolean(false);
      public boolean onePass = true;
      public static CountDownLatch latch = new CountDownLatch(0);
      
      private static String[] strings;
      
      static {
         try{ setupStream(); }
         catch (Throwable e) { logger.error("Failed to load source data",e); }
      }
      
      @Override
      public void setDispatcher(Dispatcher dispatcher) { this.dispatcher = dispatcher; }

      @Override
      public void start()
      {
         try { latch.await(); } catch (InterruptedException ie) { throw new RuntimeException(ie); }
         isRunning.set(true);
         while (isRunning.get() && !done.get())
         {
            // obtain data from an external source
            String wordString = getNextWordFromSoucre();
            dispatcher.dispatch(new Word(wordString));
         }
      }
      
      @Override
      public void stop() { isRunning.set(false); }

      private int curCount = 0;
      private String getNextWordFromSoucre()
      {
         String ret = strings[curCount++];
         if (curCount >= strings.length)
         {
            if (onePass) done.set(true);
            curCount = 0;
         }
         
         return ret;
      }

      private synchronized static void setupStream() throws IOException
      {
         if (strings == null)
         {
            InputStream is = new GZIPInputStream(new BufferedInputStream(WordProducer.class.getClassLoader().getResourceAsStream(wordResource)));
            StringWriter writer = new StringWriter();
            IOUtils.copy(is,writer);
            strings = writer.toString().split("\\s+");
         }
      }
   }
   
   @MessageProcessor
   public static class WordCounter implements Cloneable
   {
      long counter = 0;
      String wordText;
      
      @Activation
      public void initMe(String key) { this.wordText = key; }
      
      @MessageHandler
      public WordCount handle(Word word) { return new WordCount(word,counter++); }
      
      @Override
      public WordCounter clone() throws CloneNotSupportedException { return (WordCounter) super.clone(); }
   }

   @MessageProcessor
   public static class WordRank implements Cloneable
   {
      final public Map<String,Long> countMap = new ConcurrentHashMap<String, Long>();
      
      @MessageHandler
      public void handle(WordCount wordCount) { countMap.put(wordCount.word, wordCount.count); }

      @Override
      public WordRank clone() throws CloneNotSupportedException { return (WordRank) super.clone(); }
      
      public List<Pair<String,Long>> getPairs()
      {
         List<Pair<String,Long>> ret = new ArrayList<Pair<String,Long>>(countMap.size() + 10);
         for (Map.Entry<String, Long> cur : countMap.entrySet())
            ret.add(new Pair<String, Long>(cur.getKey(),cur.getValue()));
         Collections.sort(ret, new Comparator<Pair<String,Long>>()
               {
                  @Override
                  public int compare(Pair<String, Long> o1, Pair<String, Long> o2)
                  {
                     return o2.getSecond().compareTo(o1.getSecond());
                  }
            
               });
         return ret;
      }
   }
   
   public static class WordCounterKryoOptimizer implements KryoOptimizer
   {
      public static WordCounterKryoOptimizer instance = new WordCounterKryoOptimizer();
      
      @Override
      public void preRegister(Kryo kryo)
      {
         kryo.setRegistrationRequired(true);
         kryo.register(Word.class);
         kryo.register(WordCount.class);
      }

      @Override
      public void postRegister(Kryo kryo) { }
   }
   //========================================================================
   
   Set<String> finalResults = new HashSet<String>();
   {
      finalResults.addAll(Arrays.asList("the","and","of","to","And","in","that","he","shall","unto","I"));
   }
   
   @SuppressWarnings("unchecked")
   @Test
   public void testWordCount() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(ClassPathXmlApplicationContext[] contexts) throws Throwable
         {
            ClassPathXmlApplicationContext actx = contexts[0];
            Dempsy dempsy = actx.getBean(Dempsy.class);
            WordProducer adaptor = (WordProducer)TestUtils.getAdaptor(dempsy, "test-wordcount", "test-cluster0");
            adaptor.onePass = true;
            WordProducer.latch.countDown(); // let'er rip
            assertTrue(TestUtils.poll(baseTimeoutMillis * 30, adaptor, new Condition<WordProducer>()
            {
               @Override public boolean conditionMet(WordProducer o) { return o.done.get(); }
            }));
            
            // wait for the messages to get through
            Thread.sleep(100);// but not too long
            
            // now get the WordRank Mp and sort the results.
            actx = contexts[4];
            dempsy = actx.getBean(Dempsy.class);
            WordRank rank = (WordRank)TestUtils.getMp(dempsy, "test-wordcount", "test-cluster2");
            List<Pair<String,Long>> results = rank.getPairs();
            
            boolean exactCheck = !TestUtils.getReceiver(2, contexts).getFailFast();
            if (exactCheck) // we can only do this assert if there are no dropped messages
               assertTrue(finalResults.contains(results.get(0).getFirst()));
         }
         
         @Override
         public String toString() { return "testElasticityStartStop"; }
         
         @Override
         public void setup() 
         {
            WordProducer.latch = new CountDownLatch(1);
            DempsyTestBase.TestKryoOptimizer.proxy = WordCounterKryoOptimizer.instance;
         }
      },
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster0"),
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"),
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"),
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster1"),
      new Pair<String[],String>(new String[]{ actxPath }, "test-cluster2"));
   }
}
