package com.nokia.dempsy.router;

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
}
