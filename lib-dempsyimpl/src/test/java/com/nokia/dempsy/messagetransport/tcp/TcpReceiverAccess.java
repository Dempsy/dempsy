package com.nokia.dempsy.messagetransport.tcp;

import com.nokia.dempsy.executor.DempsyExecutor;

/**
 * This class is in the same package as the TcpReceiver in order to be able to access 
 * protected fields for tests.
 */
public class TcpReceiverAccess
{
   public static DempsyExecutor getExecutor(TcpReceiver r) { return r.executor; }
}
