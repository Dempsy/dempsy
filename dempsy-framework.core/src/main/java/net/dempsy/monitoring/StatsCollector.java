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

package net.dempsy.monitoring;

public interface StatsCollector extends AutoCloseable {

    /**
     * A timer context is returned from start calls on the Stats collector to provide a thread-safe context for stopping the started timer. This is analogous to Yammer Metrics use of Times.
     */
    public static interface TimerContext extends AutoCloseable {
        public void stop();

        @Override
        public default void close() {
            stop();
        }
    }

    @Override
    default public void close() {
        stop();
    }

    /**
     * Some stats collectors need to be stopped.
     */
    public void stop();

}
