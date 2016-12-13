package net.dempsy.messagetransport.tcp;

import net.dempsy.executor.DempsyExecutor;
import net.dempsy.messagetransport.tcp.TcpReceiver;

/**
 * This class is in the same package as the TcpReceiver in order to be able to access 
 * protected fields for tests.
 */
public class TcpReceiverAccess
{
   public static DempsyExecutor getExecutor(TcpReceiver r) { return r.executor; }
}
