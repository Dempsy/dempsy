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

import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {TcpSenderFactory.class, TcpTransport.class, TcpReceiver.class})
public class TcpTransportMockitoTest
{

    private TcpTransport transport;
    @Mock
    private OverflowHandler handler;
    @Mock
    private TcpReceiver receiver;

    @Before
    public void setUp() throws Exception
    {
        transport = new TcpTransport();
        transport.setOverflowHandler(handler);

    }

    @Test
    public void testCreateOutboundReturnsATcpFactory() throws Exception
    {
        TcpSenderFactory mockFactory = mock(TcpSenderFactory.class);
        PowerMockito.whenNew(TcpSenderFactory.class).withNoArguments().thenReturn(mockFactory);
        assertEquals(new TcpSenderFactory(), new TcpTransport().createOutbound());
    }

    @Test
    public void testCreatedReceiverHasTheTransportOverflowHandler() throws Exception
    {
        ensureMockReceiverAlwaysConstructed();
        Receiver inbound = transport.createInbound();
        assertReceiverIsSetupAndStarted(inbound);
    }

    private void assertReceiverIsSetupAndStarted(Receiver inbound) throws MessageTransportException
    {
        verify(receiver, times(1)).setOverflowHandler(handler);
        verify(receiver, times(1)).start();
        assertSame(inbound, receiver);
    }

    private void ensureMockReceiverAlwaysConstructed() throws Exception
    {
        PowerMockito.whenNew(TcpReceiver.class).withNoArguments().thenReturn(receiver);
    }
}
