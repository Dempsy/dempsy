/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.transport;

/**
 * A simple interface to send messages to a messaging server destination.
 */
public interface Sender {
    /**
     * Sends the message. The implementor needs to take special care to handle exceptions
     * from the underlying system correctly. The user of the <em>Sender</em> should not be
     * required to release the sender and reaquire it or another one. The Sender should do
     * that work.
     *
     * @param message
     *     The pre-serialized message to send
     * @throws MessageTransportException
     *     indicates that the message wasn't sent.
     */
    public void send(Object message) throws MessageTransportException;

    public void stop(); // this should manage everything related to the SenderFactory

    public boolean considerMessageOwnsershipTransfered();

}
