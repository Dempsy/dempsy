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

package net.dempsy.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import net.dempsy.lifecycle.annotation.Evictable;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Start;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeySource;

public class TestConfig {

    @MessageType
    public static interface Junk {

    }

    @MessageType
    public static class GoodMessage {
        @MessageKey
        public String key() {
            return "Hello";
        }
    }

    @MessageType
    public static class GoodMessageWith2Keys {
        @MessageKey("key1")
        public String key1() {
            return "Hello1";
        }

        @MessageKey("key2")
        public String key2() {
            return "Hello2";
        }
    }

    @Mp
    public static class GoodTestMp implements Cloneable {
        @MessageHandler
        public void handle(final GoodMessage string) {}

        @Start
        public void startMethod() {}

        @Evictable
        public boolean evict() {
            return false;
        }

        @Override
        public GoodTestMp clone() throws CloneNotSupportedException {
            return (GoodTestMp)super.clone();
        }
    }

    @Mp
    public static class MultiStartTestMp {
        @MessageHandler
        public void handle(final GoodMessage string) {}

        @Start
        public void startMethod() {}

        @Start
        public void extraStartMethod() {}

    }

    public static class GoodAdaptor implements Adaptor {
        @Override
        public void setDispatcher(final Dispatcher dispatcher) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

    }

    @Test
    public void testSimpleConfig() throws Throwable {
        final Node node = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();
        final Cluster cd = new Cluster("test-slot");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));

        node.addClusters(cd);

        // if we get to here without an error we should be okay
        node.validate(); // this throws if there's a problem.

        assertNotNull(node.getReceiver());
        assertNotNull(cd.getRoutingStrategyId());
        assertNotNull(node.getClusterStatsCollectorFactoryId());
    }

    @Test
    public void testSimpleConfigBuilder() throws Throwable {
        final Node node = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object())
            .cluster("test-slot").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).build();

        final Cluster cd = node.getCluster("test-slot");

        // if we get to here without an error we should be okay
        node.validate(); // this throws if there's a problem.

        assertNotNull(node.getReceiver());
        assertNotNull(cd.getRoutingStrategyId());
        assertNotNull(node.getClusterStatsCollectorFactoryId());
    }

    @Test
    public void testConfig() throws Throwable {
        final List<Cluster> clusterDefs = new ArrayList<Cluster>();

        Object appSer;
        String appRs;
        String appSc;
        final Node node = new Node.Builder("test").receiver(appSer = new Object())
            .clusterStatsCollectorFactoryId(appSc = "s").defaultRoutingStrategyId(appRs = "a").build();

        Cluster cd = new Cluster("test-slot1").adaptor(new GoodAdaptor());
        clusterDefs.add(cd);

        cd = new Cluster("test-slot2").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .destination(new ClusterId(new ClusterId("test", "test-slot3")));
        clusterDefs.add(cd);

        cd = new Cluster("test-slot3");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        cd.setDestinations(new ClusterId[] {new ClusterId("test", "test-slot4"),new ClusterId("test", "test-slot5")});
        clusterDefs.add(cd);

        cd = new Cluster("test-slot4").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        clusterDefs.add(cd);

        cd = new Cluster("test-slot5").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        clusterDefs.add(cd);

        final String clusRs = "c";
        cd = new Cluster("test-slot6").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).routingStrategyId(clusRs);
        clusterDefs.add(cd);

        cd = new Cluster("test-slot1.5").adaptor(new GoodAdaptor());
        assertNotNull(cd.getAdaptor());
        clusterDefs.add(cd);

        node.addClusters(clusterDefs);

        // if we get to here without an error we should be okay
        node.validate(); // this throws if there's a problem.

        assertTrue(node.getClusters().get(0).isAdaptor());
        assertEquals(new ClusterId("test", "test-slot2"), node.getClusters().get(1).getClusterId());
        assertEquals("test", node.getClusters().get(1).getClusterId().applicationName);
        assertEquals("test-slot2", node.getClusters().get(1).getClusterId().clusterName);
        assertEquals(new ClusterId("test", "test-slot2").hashCode(), node.getClusters().get(1).getClusterId().hashCode());
        assertFalse(new ClusterId("test", "test-slot3").equals(new Object()));
        assertFalse(new ClusterId("test", "test-slot3").equals(null));

        assertEquals(appSer, node.getReceiver());

        assertEquals(appRs, node.getDefaultRoutingStrategyId());

        assertEquals(clusRs, node.getClusters().get(5).getRoutingStrategyId());

        assertEquals(new ClusterId("test", "test-slot1"), node.getClusters().get(0).getClusterId());
        assertEquals(appSc, node.getClusterStatsCollectorFactoryId());
    }

    @Test
    public void testConfigBuilder() throws Throwable {

        Object appSer;
        String appRs;
        String appScf;
        String clusRs;
        final Node app = new Node.Builder("test")
            .receiver(appSer = new Object())
            .defaultRoutingStrategyId(appRs = "s")
            .clusterStatsCollectorFactoryId(appScf = "st")
            .cluster("test-slot1").adaptor(new GoodAdaptor())
            .cluster("test-slot2").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).destination("test-slot3")
            .cluster("test-slot3").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).destination("test-slot4", "test-slot5")
            .cluster("test-slot4").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .cluster("test-slot5").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .cluster("test-slot6").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).routingOnCluster(clusRs = "c")
            .cluster("test-slot1.5").adaptor(new GoodAdaptor())
            .build();

        assertNotNull(app.getCluster(new ClusterId("test", "test-slot1.5")).getAdaptor());
        assertNotNull(app.getCluster("test-slot1.5").getAdaptor());

        // if we get to here without an error we should be okay
        app.validate(); // this throws if there's a problem.

        assertTrue(app.getClusters().get(0).isAdaptor());
        assertEquals(new ClusterId("test", "test-slot2"), app.getClusters().get(1).getClusterId());
        assertEquals("test", app.getClusters().get(1).getClusterId().applicationName);
        assertEquals("test-slot2", app.getClusters().get(1).getClusterId().clusterName);
        assertEquals(new ClusterId("test", "test-slot2").hashCode(), app.getClusters().get(1).getClusterId().hashCode());
        assertFalse(new ClusterId("test", "test-slot3").equals(new Object()));
        assertFalse(new ClusterId("test", "test-slot3").equals(null));

        assertEquals(appSer, app.getReceiver());

        assertEquals(appRs, app.getDefaultRoutingStrategyId());

        assertEquals(clusRs, app.getClusters().get(5).getRoutingStrategyId());

        assertEquals(new ClusterId("test", "test-slot1"), app.getClusters().get(0).getClusterId());

        assertEquals(appScf, app.getClusterStatsCollectorFactoryId());
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoPrototypeOrAdaptor() throws Throwable {
        final Node app = new Node("test");
        final Cluster cd = new Cluster("test-slot1");
        app.addClusters(cd); // no prototype or adaptor
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoPrototypeOrAdaptorBuilder() throws Throwable {
        final Node node = new Node.Builder("test")
            .cluster("test-slot1").build();
        node.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBothPrototypeAndAdaptor() throws Throwable {
        final Cluster cd = new Cluster("test-slot1");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        cd.setAdaptor(new GoodAdaptor());
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBothPrototypeAndAdaptorBuilder() throws Throwable {
        new Node.Builder("test")
            .cluster("test-slot1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).adaptor(new GoodAdaptor()).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNullClusterDefinition() throws Throwable {
        new Node.Builder("test")
            .clusters(new Cluster("test-slot1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
                null,
                new Cluster("test-slot2").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNoClusterDefinition() throws Throwable {
        final Node node = new Node("test");
        node.addClusters();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupCluster() throws Throwable {
        final Node app = new Node("test-tooMuchWine-needMore").defaultRoutingStrategyId("");
        app.clusters(
            new Cluster("notTheSame").adaptor(new GoodAdaptor()),
            new Cluster("mp-stage1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
            new Cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
            new Cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
            new Cluster("mp-stage3").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())));
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupClusterBuilder() throws Throwable {
        new Node.Builder("test-tooMuchWine-needMore").defaultRoutingStrategyId("")
            .cluster("notTheSame").adaptor(new GoodAdaptor())
            .cluster("mp-stage1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .cluster("mp-stage3").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .build()
            .validate();
    }

    @Test
    public void testSimpleConfigWithKeyStore() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();
        final Cluster cd = new Cluster("test-slot");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        cd.setKeySource(new KeySource<Object>() {
            @Override
            public Iterable<Object> getAllPossibleKeys() {
                return null;
            }
        });
        app.addClusters(cd);
        app.validate();
    }

    @Test
    public void testSimpleConfigWithKeyStoreBuilder() throws Throwable {
        final Node node = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object())
            .cluster("test-slot").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
            .keySource(new KeySource<Object>() {
                @Override
                public Iterable<Object> getAllPossibleKeys() {
                    return null;
                }
            })
            .build();
        node.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStore() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").build();
        final Cluster cd = new Cluster("test-slot");
        cd.adaptor(new Adaptor() {
            @Override
            public void stop() {}

            @Override
            public void start() {}

            @Override
            public void setDispatcher(final Dispatcher dispatcher) {}
        }).setKeySource(new KeySource<Object>() {
            @Override
            public Iterable<Object> getAllPossibleKeys() {
                return null;
            }
        });
        app.addClusters(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStoreBuilder() throws Throwable {
        final Node node = new Node.Builder("test").defaultRoutingStrategyId("")
            .cluster("test-slot").adaptor(new Adaptor() {
                @Override
                public void stop() {}

                @Override
                public void start() {}

                @Override
                public void setDispatcher(final Dispatcher dispatcher) {}
            }).keySource(new KeySource<Object>() {
                @Override
                public Iterable<Object> getAllPossibleKeys() {
                    return null;
                }
            })
            .build();
        node.validate();
    }

    @Test
    public void testConfigMpWithGoodWith2Keys() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();

        @Mp
        class mp1 implements Cloneable {
            @MessageHandler("key1")
            public void handle(final GoodMessageWith2Keys string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        @Mp
        class mp2 implements Cloneable {
            @MessageHandler("key2")
            public void handle(final GoodMessageWith2Keys string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Cluster cd1 = new Cluster("test-slot-1");
        final Cluster cd2 = new Cluster("test-slot-2");
        cd1.setMessageProcessor(new MessageProcessor<mp1>(new mp1()));
        cd2.setMessageProcessor(new MessageProcessor<mp2>(new mp2()));
        app.addClusters(cd1);
        app.addClusters(cd2);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithGoodWith2KeysButMpFailsToPickOne() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();

        @Mp
        class mp1 implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessageWith2Keys string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Cluster cd1 = new Cluster("test-slot-1");
        cd1.setMessageProcessor(new MessageProcessor<mp1>(new mp1()));
        app.addClusters(cd1);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithGoodButMpSelects() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();

        @Mp
        class mp1 implements Cloneable {
            @MessageHandler("key1")
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Cluster cd1 = new Cluster("test-slot-1");
        cd1.setMessageProcessor(new MessageProcessor<mp1>(new mp1()));
        app.addClusters(cd1);
        app.validate();
    }

    @Test
    public void testConfigMpWithGoodMPEvict() throws Throwable {
        final Node app = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object()).build();
        final Cluster cd = new Cluster("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        cd.setMessageProcessor(new MessageProcessor<mp>(new mp()));
        app.addClusters(cd);
        app.validate();
    }

    @Test
    public void testConfigMpWithGoodMPEvictBuilder() throws Throwable {

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Node node = new Node.Builder("test").defaultRoutingStrategyId("").receiver(new Object())
            .cluster("slot").mp(new MessageProcessor<mp>(new mp())).build();
        node.validate();
    }

    // TODO:
    // =================================================================
    // These need to be moved to an annotation MessageProcessor test
    //
    // @Test(expected = IllegalStateException.class)
    // public void testFailBadPrototype() throws Throwable {
    // final Node app = new Node("test");
    // final ClusterDefinition cd = new ClusterDefinition("test-slot1");
    // cd.setMessageProcessor(new MessageProcessor(new Object())); // has no annotated methods
    // app.setClusters(cd);
    // app.validate();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testFailBadPrototypeBuilder() throws Throwable {
    // final Node node = new Node("test");
    // node.cluster("test-slot1").mp(new MessageProcessor(new Object()));
    // node.validate();
    // }
    //
    //
    //
    // @Test(expected = IllegalStateException.class)
    // public void testMultipleStartMethodsDisallowed() throws Throwable {
    // final ApplicationDefinition app = new ApplicationDefinition("test-multiple-starts").add(
    // new ClusterDefinition("adaptor").setAdaptor(new GoodAdaptor()),
    // new ClusterDefinition("good-mp").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
    // new ClusterDefinition("bad-mp").setMessageProcessor(new MessageProcessor(new MultiStartTestMp())));
    // app.validate();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testMultipleStartMethodsDisallowedTopology() throws Throwable {
    // new Node("test-multiple-starts").add("adaptor", new GoodAdaptor())
    // .add(new CdBuild("good-mp", new GoodTestMp()).cd())
    // .add(new CdBuild("bad-mp", new MultiStartTestMp()).cd()).app();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithMultipleEvict() throws Throwable {
    // final ApplicationDefinition app = new ApplicationDefinition("test");
    // final ClusterDefinition cd = new ClusterDefinition("test-slot");
    //
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public boolean evict2() {
    // return false;
    // }
    //
    // @Evictable
    // public boolean evict1() {
    // return false;
    // }
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    //
    // }
    //
    // cd.setMessageProcessor(new MessageProcessor(new mp()));
    // app.add(cd);
    // app.validate();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithMultipleEvictTopology() throws Throwable {
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public boolean evict2() {
    // return false;
    // }
    //
    // @Evictable
    // public boolean evict1() {
    // return false;
    // }
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    // }
    //
    // new Node("test").add(new CdBuild("slot", new mp()).cd()).app();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithWrongReturnTypeEvict1() throws Throwable {
    // final ApplicationDefinition app = new ApplicationDefinition("test");
    // final ClusterDefinition cd = new ClusterDefinition("test-slot");
    //
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public void evict1() {}
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    // }
    //
    // cd.setMessageProcessor(new MessageProcessor(new mp()));
    // app.add(cd);
    // app.validate();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithWrongReturnTypeEvict1Topology() throws Throwable {
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public void evict1() {}
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    // }
    // new Node("test").add(new CdBuild("slot", new mp()).cd()).app();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithWrongReturnTypeEvict2() throws Throwable {
    // final ApplicationDefinition app = new ApplicationDefinition("test");
    // final ClusterDefinition cd = new ClusterDefinition("test-slot");
    //
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public Object evict1() {
    // return null;
    // }
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    // }
    //
    // cd.setMessageProcessor(new MessageProcessor(new mp()));
    // app.add(cd);
    // app.validate();
    // }
    //
    // @Test(expected = IllegalStateException.class)
    // public void testConfigMpWithWrongReturnTypeEvict2Topology() throws Throwable {
    // @Mp
    // class mp implements Cloneable {
    // @MessageHandler
    // public void handle(final GoodMessage string) {}
    //
    // @Start
    // public void startMethod() {}
    //
    // @Evictable
    // public Object evict1() {
    // return null;
    // }
    //
    // @Override
    // public Object clone() throws CloneNotSupportedException {
    // return super.clone();
    // }
    // }
    // new Node("test").add(new CdBuild("slot", new mp()).cd()).app();
    // }
}
