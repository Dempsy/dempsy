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

import java.util.function.Supplier;

/**
 * <p>
 * This is the core abstraction for receiving messages. The client side of a transport implementation (called an "Adaptor") needs to be wired to a MessageTransportListener
 * </p>
 */
public interface Listener<T> extends AutoCloseable {
    /**
     * <p>
     * Method that accepts the callback for received messages. Given that the transport is responsible for managing threads, the transport will also let the Listener implementation know if it should make every
     * effort to handle the request or if it should "fail-fast."
     * </p>
     * 
     * @param message
     *            The message received
     * @throws MessageTransportException
     */
    public boolean onMessage(T message) throws MessageTransportException;

    public default boolean onMessage(final Supplier<T> supplier) {
        return onMessage(supplier.get());
    }

    @Override
    public default void close() {}

}
