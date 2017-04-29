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
import net.dempsy.Service;

/**
 * Abstraction to create multiple sender based on destination.
 */
public interface SenderFactory extends Service {

    public Sender getSender(NodeAddress destination) throws MessageTransportException;

    @Override
    public default void stop() {
        close();
    }

    @Override
    public void start(final Infrastructure infra);

}
