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

package net.dempsy.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.LongSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import net.dempsy.TestUtils;
import net.dempsy.serialization.java.JavaSerializer;
import net.dempsy.serialization.kryo.KryoOptimizer;
import net.dempsy.serialization.kryo.KryoSerializer;
import net.dempsy.serialization.kryo.Registration;

public class TestDefaultSerializer {
    private static final int TEST_NUMBER = 42;
    private static final String TEST_STRING = "life, the universe and everything";
    private static final long baseTimeoutMillis = 20000;
    private static final int numThreads = 5;

    private final MockClass o1 = new MockClass(TEST_NUMBER, TEST_STRING);

    @Test
    public void testJavaSerializeDeserialize() throws Throwable {
        runSerializer(new JavaSerializer());
    }

    @Test
    public void testKryoSerializeDeserialize() throws Throwable {
        runSerializer(new KryoSerializer());
    }

    @Test
    public void testKryoSerializeDeserializeWithRegister() throws Throwable {
        final KryoSerializer ser1 = new KryoSerializer();
        runSerializer(ser1);
        final KryoSerializer ser2 = new KryoSerializer(true, new Registration(MockClass.class.getName(), 10));
        runSerializer(ser2);

        final byte[] d1 = ser1.serialize(o1);
        final byte[] d2 = ser2.serialize(o1);
        assertTrue(d2.length < d1.length);
    }

    @Test(expected = IOException.class)
    public void testKryoSerializeWithRegisterFail() throws Throwable {
        final KryoSerializer ser1 = new KryoSerializer();
        ser1.setKryoRegistrationRequired(true);
        runSerializer(ser1);
    }

    @Test(expected = IOException.class)
    public void testKryoDeserializeWithRegisterFail() throws Throwable {
        final KryoSerializer ser1 = new KryoSerializer();
        final KryoSerializer ser2 = new KryoSerializer();
        ser2.setKryoRegistrationRequired(true);
        final byte[] data = ser1.serialize(new MockClass());
        ser2.deserialize(data, MockClass.class);
    }

    private void runSerializer(final Serializer serializer) throws Throwable {
        final byte[] data = serializer.serialize(o1);
        assertNotNull(data);
        final MockClass o2 = serializer.deserialize(data, MockClass.class);
        assertNotNull(o2);
        assertEquals(o1, o2);
    }

    @SuppressWarnings("serial")
    public static class Mock2 implements Serializable {
        private final int i;
        private final MockClass mock;

        public Mock2() {
            i = 5;
            mock = null;
        }

        public Mock2(final int i, final MockClass mock) {
            this.i = i;
            this.mock = mock;
        }

        public int getInt() {
            return i;
        }

        public MockClass getMockClass() {
            return mock;
        }

        @Override
        public boolean equals(final Object obj) {
            final Mock2 o = (Mock2) obj;
            return o.i == i && mock.equals(o.mock);
        }
    }

    @SuppressWarnings("serial")
    public static class Mock3 extends Mock2 {
        public int myI = -1;
        private final UUID uuid = UUID.randomUUID();

        public Mock3() {}

        public Mock3(final int i, final MockClass mock) {
            super(i, mock);
            this.myI = i;
        }

        @Override
        public boolean equals(final Object obj) {
            final Mock3 o = (Mock3) obj;
            return super.equals(obj) && myI == o.myI && uuid.equals(o.uuid);
        }

        public UUID getUUID() {
            return uuid;
        }
    }

    @Test
    public void testKryoWithFinalFields() throws Throwable {
        final KryoSerializer ser = new KryoSerializer();
        final Mock2 o = new Mock2(1, new MockClass(2, "Hello"));
        final byte[] data = ser.serialize(o);
        final Mock2 o2 = ser.deserialize(data, Mock2.class);
        assertEquals(1, o2.getInt());
        assertEquals(new MockClass(2, "Hello"), o2.getMockClass());
    }

    com.esotericsoftware.kryo.Serializer<UUID> uuidSerializer = new com.esotericsoftware.kryo.Serializer<UUID>(true, true) {
        LongSerializer longSerializer = new LongSerializer();
        {
            longSerializer.setImmutable(true);
            longSerializer.setAcceptsNull(false);
        }

        @Override
        public UUID read(final Kryo kryo, final Input input, final Class<UUID> clazz) {
            final long mostSigBits = longSerializer.read(kryo, input, long.class);
            final long leastSigBits = longSerializer.read(kryo, input, long.class);
            return new UUID(mostSigBits, leastSigBits);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final UUID uuid) {
            final long mostSigBits = uuid.getMostSignificantBits();
            final long leastSigBits = uuid.getLeastSignificantBits();
            longSerializer.write(kryo, output, mostSigBits);
            longSerializer.write(kryo, output, leastSigBits);
        }
    };

    KryoOptimizer defaultMock3Optimizer = new KryoOptimizer() {
        @Override
        public void preRegister(final Kryo kryo) {}

        @Override
        public void postRegister(final Kryo kryo) {
            final com.esotericsoftware.kryo.Registration reg = kryo.getRegistration(UUID.class);
            reg.setSerializer(uuidSerializer);
        }
    };

    @Test
    public void testChildClassSerialization() throws Throwable {
        final KryoSerializer ser = new KryoSerializer(true, defaultMock3Optimizer);

        final Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
        final byte[] data = ser.serialize(o);
        final Mock2 o2 = ser.deserialize(data, Mock2.class);
        assertEquals(1, o2.getInt());
        assertEquals(new MockClass(2, "Hello"), o2.getMockClass());
        assertTrue(o2 instanceof Mock3);
        assertEquals(1, ((Mock3) o2).myI);
    }

    @Test
    public void testChildClassSerializationWithRegistration() throws Throwable {
        final KryoSerializer ser = new KryoSerializer(true, defaultMock3Optimizer);
        final JavaSerializer serJ = new JavaSerializer();
        final KryoSerializer serR = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName(), 10));
        final KryoSerializer serRR = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName(), 10),
                new Registration(Mock3.class.getName(), 11));
        final KryoSerializer serRROb = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName()),
                new Registration(Mock3.class.getName()), new Registration(UUID.class.getName()));
        final Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
        final byte[] data = ser.serialize(o);
        final byte[] dataJ = serJ.serialize(o);
        final byte[] dataR = serR.serialize(o);
        final byte[] dataRR = serRR.serialize(o);
        final byte[] dataRROb = serRROb.serialize(o);
        assertTrue(dataJ.length > data.length);
        assertTrue(dataR.length < data.length);
        assertTrue(dataRR.length < dataR.length);
        assertTrue(dataRROb.length == dataRR.length);
        final Mock2 o2 = ser.deserialize(data, Mock2.class);
        assertEquals(1, o2.getInt());
        assertEquals(new MockClass(2, "Hello"), o2.getMockClass());
        assertTrue(o2 instanceof Mock3);
        assertEquals(1, ((Mock3) o2).myI);
        serRROb.deserialize(dataRROb, Object.class);
    }

    @Test
    public void testChildClassSerializationWithRegistrationAndOptimization() throws Throwable {
        final KryoSerializer ser = new KryoSerializer(true, defaultMock3Optimizer);
        final JavaSerializer serJ = new JavaSerializer();
        final KryoSerializer serR = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName(), 10));
        final KryoSerializer serRR = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName(), 10),
                new Registration(Mock3.class.getName(), 11));
        final KryoSerializer serRROb = new KryoSerializer(true, defaultMock3Optimizer, new Registration(MockClass.class.getName()),
                new Registration(Mock3.class.getName()));
        final KryoSerializer serRRO = new KryoSerializer(true, new Registration(MockClass.class.getName(), 10),
                new Registration(Mock3.class.getName(), 11), new Registration(UUID.class.getName(), 12));
        serRRO.setKryoOptimizer(new KryoOptimizer() {
            @Override
            public void preRegister(final Kryo kryo) {
                kryo.setRegistrationRequired(true);

            }

            @Override
            public void postRegister(final Kryo kryo) {
                @SuppressWarnings("unchecked")
                final FieldSerializer<MockClass> mockClassSer = (FieldSerializer<MockClass>) kryo.getSerializer(MockClass.class);
                mockClassSer.setFieldsCanBeNull(false);
                @SuppressWarnings("unchecked")
                final FieldSerializer<Mock2> mock2Ser = (FieldSerializer<Mock2>) kryo.getSerializer(MockClass.class);
                mock2Ser.setFixedFieldTypes(true);
                mock2Ser.setFieldsCanBeNull(false);

                final com.esotericsoftware.kryo.Registration reg = kryo.getRegistration(UUID.class);
                reg.setSerializer(uuidSerializer);
            }
        });

        final Mock2 o = new Mock3(1, new MockClass(2, "Hello"));
        final byte[] data = ser.serialize(o);
        final byte[] dataJ = serJ.serialize(o);
        final byte[] dataR = serR.serialize(o);
        final byte[] dataRR = serRR.serialize(o);
        final byte[] dataRROb = serRROb.serialize(o);
        final byte[] dataRRO = serRRO.serialize(o);
        assertTrue(dataJ.length > data.length);
        assertTrue(dataR.length < data.length);
        assertTrue(dataRR.length < dataR.length);
        assertTrue(dataRROb.length == dataRR.length);
        assertTrue(dataRRO.length <= dataRR.length);
        final Mock2 o2 = ser.deserialize(data, Mock2.class);
        assertEquals(1, o2.getInt());
        assertEquals(new MockClass(2, "Hello"), o2.getMockClass());
        assertTrue(o2 instanceof Mock3);
        assertEquals(1, ((Mock3) o2).myI);
        assertEquals(o, serR.deserialize(dataR, Mock2.class));
        assertEquals(o, serRR.deserialize(dataRR, Mock2.class));
        assertEquals(o, serRRO.deserialize(dataRRO, Mock2.class));
    }

    @Test
    public void testCollectionSerialization() throws Throwable {
        final KryoSerializer ser = new KryoSerializer(true, defaultMock3Optimizer);
        final ArrayList<Mock2> mess = new ArrayList<Mock2>();
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0)
                mess.add(new Mock3(i, new MockClass(-i, "Hello:" + i)));
            else
                mess.add(new Mock2(i, new MockClass(-i, "Hello:" + i)));
        }
        final byte[] data = ser.serialize(mess);
        @SuppressWarnings("unchecked")
        final List<Mock2> des = ser.deserialize(data, List.class);
        assertEquals(mess, des);
    }

    @Test
    public void testMultithreadedSerialization() throws Throwable {
        final Thread[] threads = new Thread[numThreads];

        final Object latch = new Object();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        final KryoSerializer ser = new KryoSerializer(true, new Registration(MockClass.class.getName(), 10));
        final AtomicBoolean[] finished = new AtomicBoolean[numThreads];
        final AtomicLong[] counts = new AtomicLong[numThreads];
        final long maxSerialize = 100000;

        try {
            for (int i = 0; i < threads.length; i++) {

                finished[i] = new AtomicBoolean(true);
                counts[i] = new AtomicLong(0);

                final int curIndex = i;

                final Thread t = new Thread(new Runnable() {
                    int index = curIndex;

                    @Override
                    public void run() {
                        try {
                            synchronized (latch) {
                                finished[index].set(false);
                                latch.wait();
                            }

                            while (!done.get()) {
                                final MockClass o = new MockClass(index, "Hello:" + index);
                                final byte[] data = ser.serialize(o);
                                final MockClass dser = ser.deserialize(data, MockClass.class);
                                assertEquals(o, dser);
                                counts[index].incrementAndGet();
                            }
                        } catch (final Throwable th) {
                            failed.set(true);
                        } finally {
                            finished[index].set(true);
                        }
                    }
                }, "Kryo-Test-Thread-" + i);

                t.setDaemon(true);
                t.start();
                threads[i] = t;
            }

            // wait until all the threads have been started.
            assertTrue(TestUtils.poll(baseTimeoutMillis, finished, new TestUtils.Condition<AtomicBoolean[]>() {
                @Override
                public boolean conditionMet(final AtomicBoolean[] o) throws Throwable {
                    for (int i = 0; i < numThreads; i++)
                        if (o[i].get())
                            return false;
                    return true;
                }
            }));

            Thread.sleep(10);

            synchronized (latch) {
                latch.notifyAll();
            }

            // wait until so many message have been serialized
            // This can be slow on cloudbees servers so we're going to double the wait time.
            assertTrue(TestUtils.poll(baseTimeoutMillis * 2, counts, new TestUtils.Condition<AtomicLong[]>() {
                @Override
                public boolean conditionMet(final AtomicLong[] cnts) throws Throwable {
                    for (int i = 0; i < numThreads; i++)
                        if (cnts[i].get() < maxSerialize)
                            return false;
                    return true;
                }

            }));
        } finally {
            done.set(true);
        }

        for (int i = 0; i < threads.length; i++)
            threads[i].join(baseTimeoutMillis);

        for (int i = 0; i < threads.length; i++)
            assertTrue(finished[i].get());

        assertTrue(!failed.get());
    }

    public static enum MockEnum {
        FROM("F"),
        TO("T"),
        BOTH("B");

        private final String publicValue;

        private MockEnum(final String value) {
            publicValue = value;
        }

        @Override
        public String toString() {
            return publicValue;
        }
    }

    public static class Mock4 {
        private MockEnum e;

        public Mock4() {}

        public Mock4(final MockEnum e) {
            this.e = e;
        }

        public MockEnum get() {
            return e;
        }
    }

    @Test
    public void testEnumWithNoDefaultConstructor() throws Throwable {
        final Mock4 o = new Mock4(MockEnum.BOTH);
        final KryoSerializer ser = new KryoSerializer(true, new Registration(Mock4.class.getName()), new Registration(MockEnum.class.getName()));
        final byte[] data = ser.serialize(o);
        final Mock4 dser = ser.deserialize(data, Mock4.class);
        assertTrue(o.get() == dser.get());
        assertEquals(o.get().toString(), dser.get().toString());
    }
}
