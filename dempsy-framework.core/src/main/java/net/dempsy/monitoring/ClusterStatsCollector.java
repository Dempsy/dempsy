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

import java.awt.Container;

public interface ClusterStatsCollector extends StatsCollector {

    public class Transaction implements AutoCloseable {
        private boolean failed = false;
        private boolean mpFailure = false;
        private final Object message;
        private final ClusterStatsCollector ths;

        private Transaction(final ClusterStatsCollector ths, final Object message) {
            this.message = message;
            this.ths = ths;
            ths.messageDispatched(message);
        }

        public void failed(final boolean mpFailure) {
            failed = true;
            this.mpFailure = mpFailure;
        }

        @Override
        public void close() {
            if (failed)
                ths.messageFailed(mpFailure);
            else ths.messageProcessed(message);
        }
    }

    public default Transaction messageProcessTransaction(final Object message) {
        return new Transaction(this, message);
    }

    /**
     * {@link Container} calls this method before invoking an MP's <code>MessageHandler</code>
     * or Output method.
     * 
     * A message processing "transaction" opens with a messageDispatched. It
     * the closes with one of the following:
     * <ul>
     * <li>messageProcessed - successfully handled</li>
     * <li>messageFailed - there was an error</li>
     * </ul>
     */
    void messageDispatched(Object message);

    /**
     * {@link Container} calls this method when successfully invoking an MP's <code>MessageHandler</code> or Output method.
     */
    void messageProcessed(Object message);

    /**
     * {@link Container} calls this method when invoking an MP's <code>MessageHandler</code> or Output method results in an error.
     */
    void messageFailed(boolean mpFailure);

    /**
     * The dispatcher calls this method in its <code>onMessage</code> handler when it discards a message due to a collision at the Mp. This number will ALSO be reflected in the messageDiscarded results.
     */
    void messageCollision(Object message);

    /**
     * The MP manager calls this method when it creates a message processor instance.
     */
    void messageProcessorCreated(Object key);

    /**
     * The instance manager calls this method when it deletes a message processor instance.
     */
    void messageProcessorDeleted(Object key);

    /**
     * Some stats collectors need to be stopped.
     */
    @Override
    public void stop();

    /**
     * Dempsy calls into this just before starting pre-instantiation.
     */
    public TimerContext preInstantiationStarted();

    /**
     * Dempsy calls into this just before calling @Output methods for MPs.
     */
    public TimerContext outputInvokeStarted();

    /**
     * Dempsy calls into this just before calling @Output methods for MPs.
     */
    public TimerContext evictionPassStarted();

    @Override
    default public void close() {
        stop();
    }

}
