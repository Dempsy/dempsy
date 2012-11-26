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

import com.nokia.dempsy.messagetransport.Destination;

public class TcpDestination implements Destination, Serializable
{
   private static final long serialVersionUID = 1L;
   
   protected InetAddress inetAddress;
   protected int port;
   protected int sequence = 0;
   
   public TcpDestination() {   }
   
   public TcpDestination(TcpDestination destination, int sequence)
   {
      this.inetAddress = destination.inetAddress;
      this.port = destination.port;
      this.sequence = sequence;
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
      return "(" + inetAddress.getHostAddress() + (port > 0 ? (":" + port + ")") : "");
   }
   
   public TcpDestination baseDestination() {  return new TcpDestination(inetAddress,port); }
   
   @Override
   public boolean equals(Object other)
   {
      if (other == null)
         return false;
      TcpDestination otherTcpDestination = (TcpDestination)other;
      return inetAddress.equals(otherTcpDestination.inetAddress ) && (port == otherTcpDestination.port) && (sequence == otherTcpDestination.sequence);
   }
   
   @Override
   public int hashCode() { return inetAddress.hashCode() ^ port ^ (int)sequence; }
   
   /**
    * Get the IP address we are listening to.
    * Called by StatsCollectorFactory to make pretty names for nodes.
    * @return the InternetAddress of teh listner.
    */
   public InetAddress getInetAddress() { return inetAddress; }

   public void setInetAddress(InetAddress inetAddress) { this.inetAddress = inetAddress; }

   public void setPort(int port) { this.port = port; }
   
   public int getPort() { return this.port; }
   
   public int getSequence() { return this.sequence; }
   
   public void setSequence(int sequence) { this.sequence = sequence; }
}
