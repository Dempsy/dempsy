package com.nokia.dempsy.router;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

import com.nokia.dempsy.config.ClusterId;

public class TestRegExClusterCheck
{

   @Test
   public void testRegExpClusterCheckAlwaysMatch()
   {
      RegExClusterCheck check = new RegExClusterCheck("app", "^.*$"); // this should match anything
      RegExClusterCheck check2 = new RegExClusterCheck(new ClusterId("app","^.*$")); // use both constructors
    
      for (int j = 0; j < 100; j++)
      {
         String randomString = new String();

         Random rng = new Random();
         int len = rng.nextInt(500) + 1; // random # from 1 to 500 (inclusive)

         for (int i = 0; i < len; i++)
            randomString += (char)(rng.nextInt(126 - 33) + 33); // random character in the valid ascii range.

         assertTrue(check.isThisNodePartOfCluster(new ClusterId("app",randomString)));
         assertTrue(check2.isThisNodePartOfCluster(new ClusterId("app",randomString)));
      }
   }
   
   @Test
   public void testRegExpClusterCheckNeverMatch()
   {
      RegExClusterCheck check = new RegExClusterCheck("app", "A"); // this shouldn't match anything
                                                                   //  since all attempts will be made against
                                                                   //  clusternames bigger than one character.
      RegExClusterCheck check2 = new RegExClusterCheck(new ClusterId("app","A")); // use both constructors
    
      for (int j = 0; j < 100; j++)
      {
         String randomString = "A";

         Random rng = new Random();
         int len = rng.nextInt(500) + 2; // random # from 1 to 500 (inclusive)

         for (int i = 0; i < len; i++)
            randomString += (char)(rng.nextInt(126 - 33) + 33); // random character in the valid ascii range.
         
         assertFalse(check.isThisNodePartOfCluster(new ClusterId("app",randomString)));
         assertFalse(check2.isThisNodePartOfCluster(new ClusterId("app",randomString)));
      }
   }
}
