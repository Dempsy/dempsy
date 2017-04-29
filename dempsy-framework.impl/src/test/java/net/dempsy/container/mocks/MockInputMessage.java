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

package net.dempsy.container.mocks;

import java.io.Serializable;

import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;

@SuppressWarnings("serial")
@MessageType
public class MockInputMessage implements Serializable {

    private String key;
    private boolean processed = false;

    @SuppressWarnings("unused")
    private MockInputMessage() {}

    public MockInputMessage(final String key) {
        this.key = key;
    }

    @MessageKey
    public String getKey() {
        return key;
    }

    public void setProcessed(final boolean processed) {
        this.processed = processed;
    }

    public boolean isProcessed() {
        return processed;
    }

    @Override
    public String toString() {
        return "TestInputMessage[ key = " + key + ", processed = " + processed + "]";
    }
}
