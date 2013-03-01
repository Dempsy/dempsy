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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.util.ReceiverIndexedDestination;

public class TcpDestination extends ReceiverIndexedDestination implements Destination, Serializable
{
   private static final long serialVersionUID = 1L;
   
   protected InetAddress inetAddress;
   protected int port;
   
   public final static TcpDestination createNewDestinationForThisHost(int port, boolean useLocalhost) throws MessageTransportException
   {
      try
      {
         InetAddress inetAddress = useLocalhost ? InetAddress.getLocalHost() : TcpTransport.getFirstNonLocalhostInetAddress();
         if (inetAddress == null)
            throw new MessageTransportException("Failed to set the Inet Address for this host. Is there a valid network interface?");
         
         return new TcpDestination(inetAddress, port);
      }
      catch(UnknownHostException e)
      {
         throw new MessageTransportException("Failed to identify the current hostname", e);
      }
      catch(SocketException e)
      {
         throw new MessageTransportException("Failed to identify the current hostname", e);
      }
   }

   public TcpDestination() {   }
   
   public TcpDestination(TcpDestination destination, int sequence)
   {
      super(sequence);
      this.inetAddress = destination.inetAddress;
      this.port = destination.port;
   }
   
   protected TcpDestination(InetAddress inetAddress, int port)
   {
      this.inetAddress = inetAddress;
      this.port = port;
   }
   
   protected boolean isEphemeral() { return port <= 0; } 
   
   @Override
   public String toString()
   {
      return "(" + inetAddress.getHostAddress() + (port > 0 ? (":" + port) : "") + "[" + getReceiverIndex() + "])";
   }
   
   public TcpDestination baseDestination() {  return new TcpDestination(inetAddress,port); }
   
   @Override
   public boolean equals(Object other)
   {
      if (other == null)
         return false;
      TcpDestination otherTcpDestination = (TcpDestination)other;
      return inetAddress.equals(otherTcpDestination.inetAddress ) && (port == otherTcpDestination.port) && super.equals(other);
   }
   
   @Override
   public int hashCode() { return inetAddress.hashCode() ^ port ^ super.hashCode(); }
   
   /**
    * Get the IP address we are listening to.
    * Called by StatsCollectorFactory to make pretty names for nodes.
    * @return the InternetAddress of teh listner.
    */
   public InetAddress getInetAddress() { return inetAddress; }

   public void setInetAddress(InetAddress inetAddress) { this.inetAddress = inetAddress; }

   public void setPort(int port) { this.port = port; }
   
   public int getPort() { return this.port; }
}
