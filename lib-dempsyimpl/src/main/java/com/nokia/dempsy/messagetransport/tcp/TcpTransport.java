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

package com.nokia.dempsy.messagetransport.tcp;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;

public class TcpTransport implements Transport
{
   private OverflowHandler overflowHandler = null;
   private boolean failFast = true;

   @Override
   public SenderFactory createOutbound(DempsyExecutor executor) throws MessageTransportException
   {
      return new TcpSenderFactory();
   }

   @Override
   public Receiver createInbound(DempsyExecutor executor) throws MessageTransportException
   {
      TcpReceiver receiver = new TcpReceiver();
      receiver.setOverflowHandler(overflowHandler);
      receiver.setFailFast(failFast);
      receiver.start(executor);
      return receiver;
   }

   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler) throws MessageTransportException
   {
      this.overflowHandler = overflowHandler;
   }
   
   public void setFailFast(boolean failFast) { this.failFast = failFast; }
   
   public static InetAddress getFirstNonLocalhostInetAddress() throws SocketException
   {
      Enumeration<NetworkInterface> netInterfaces=NetworkInterface.getNetworkInterfaces();
      while(netInterfaces.hasMoreElements()){
         NetworkInterface networkInterface = (NetworkInterface)netInterfaces.nextElement();
         for (Enumeration<InetAddress> loopInetAddress = networkInterface.getInetAddresses(); loopInetAddress.hasMoreElements(); )
         {
            InetAddress tempInetAddress = loopInetAddress.nextElement();
            if (!tempInetAddress.isLoopbackAddress() && tempInetAddress instanceof Inet4Address)
               return tempInetAddress;
         }
      }
      return null;
   }
   
   public static InetAddress getInetAddressBestEffort()
   {
      InetAddress ret = null;
      
      try { ret = getFirstNonLocalhostInetAddress(); }
      catch (SocketException e) { ret = null; }
      
      if (ret == null)
      {
         try { ret = InetAddress.getLocalHost(); }
         catch (UnknownHostException e) { ret = null; }
      }
      return ret;
   }

}
