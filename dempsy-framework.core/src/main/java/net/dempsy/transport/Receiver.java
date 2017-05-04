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

package net.dempsy.transport;

import net.dempsy.Infrastructure;

public interface Receiver extends AutoCloseable {
    /**
     * What address can a Sender use to send messages to this receiver. This is called PRIOR to start
     */
    public NodeAddress getAddress();

    /**
     * A receiver is started with a Listener and a threading model.
     */
    public void start(Listener<?> listener, Infrastructure threadingModel) throws MessageTransportException;

    /**
     * What is a unique Id for the transport that this {@link Receiver} is associated with. This information is used
     * by the TransportManager to look up a {@link SenderFactory} that's compatible with this {@link Receiver}. The default
     * behavior for this method is to provide the package name of the implementing class
     */
    public default String transportTypeId() {
        return this.getClass().getPackage().getName();
    }
}
