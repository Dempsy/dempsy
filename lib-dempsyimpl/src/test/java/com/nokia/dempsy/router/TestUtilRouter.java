package com.nokia.dempsy.router;

import java.util.Collection;

import com.nokia.dempsy.router.RoutingStrategy.Outbound;

/**
 * This class allows access to Router package/protected fields for other tests that 
 * are not part of the this package.
 */
public class TestUtilRouter
{
   /**
    * This method will take the class type and count the number of known Outbounds for 
    * the given type.
    */
   public static int numberOfKnownDestinationsForMessage(Router router, Class<?> messageType)
   {
      return router.outboundManager.retrieveOutbounds(messageType).size();
   }
   
   public static boolean allOutboundsInitialized(Router router, Class<?> messageType)
   {
      Collection<Outbound> outbounds = router.outboundManager.retrieveOutbounds(messageType);
      if (outbounds == null || outbounds.size() == 0)
         return false;
      for (Outbound ob : outbounds)
         if (!ob.completeInitialization())
            return false;
      return true;
   }
}
