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

package net.dempsy.messages;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import net.dempsy.DempsyException;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;

/**
 * <p>
 * Implementations of this interface accept messages pushed from a source.
 * This is primarily exposed to the application developer to support the
 * development of {@link Adaptor}s. An adaptor needs to take data from an external
 * source, and provide a routable message (one with a {@link MessageKey}) to the
 * Dempsy framework. {@link Adaptor}s will be provided a {@link Dispatcher},
 * which constitutes a handle to the Dempsy message bus.
 * </p>
 */
public abstract class Dispatcher {
    private final KeyExtractor extractor = new KeyExtractor();

    /**
     * An {@link Adaptor} will use this method to send a "routable message" to
     * a {@link Mp} somewhere in the Dempsy application that the
     * {@link Adaptor} is part of.
     */
    public abstract void dispatch(KeyedMessageWithType message) throws DempsyException;

    public void dispatchAnnotated(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if(message == null)
            throw new NullPointerException("Cannot dispatch a null message.");

        dispatch(extractor.extract(message));
    }

    public void dispatch(final List<KeyedMessageWithType> messages) {
        if(messages != null)
            messages.forEach(v -> dispatch(v));
    }

}
