package com.nokia.dempsy;

public class TestUtils
{
   public static interface Condition<T>
   {
      public boolean conditionMet(T o);
   }

   public static <T> boolean poll(long timeoutMillis, T userObject, Condition<T> condition) throws InterruptedException
   {
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !condition.conditionMet(userObject);)
         Thread.sleep(1);
      return condition.conditionMet(userObject);
   }
   

}
