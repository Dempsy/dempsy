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
            return (GoodTestMp) super.clone();
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
        final Node node = new Node("test").setDefaultRoutingStrategyId("").receiver(new Object());
        final Cluster cd = new Cluster("test-slot");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));

        node.setClusters(cd);

        // if we get to here without an error we should be okay
        node.validate(); // this throws if there's a problem.

        assertNotNull(node.getReceiver());
        assertNotNull(cd.getRoutingStrategyId());
        assertNotNull(node.getClusterStatsCollectorFactoryId());
    }

    @Test
    public void testSimpleConfigBuilder() throws Throwable {
        final Node node = new Node("test").setDefaultRoutingStrategyId("").receiver(new Object());
        final Cluster cd = node.cluster("test-slot").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));

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
        final Node node = new Node("test").receiver(appSer = new Object())
                .setClusterStatsCollectorFactoryId(appSc = "s").setDefaultRoutingStrategyId(appRs = "a");

        Cluster cd = new Cluster("test-slot1").setAdaptor(new GoodAdaptor());
        clusterDefs.add(cd);

        cd = new Cluster("test-slot2").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
                .setDestinations(new ClusterId(new ClusterId("test", "test-slot3")));
        clusterDefs.add(cd);

        cd = new Cluster("test-slot3");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        cd.setDestinations(new ClusterId[] { new ClusterId("test", "test-slot4"), new ClusterId("test", "test-slot5") });
        clusterDefs.add(cd);

        cd = new Cluster("test-slot4").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        clusterDefs.add(cd);

        cd = new Cluster("test-slot5").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        clusterDefs.add(cd);

        final String clusRs = "c";
        cd = new Cluster("test-slot6").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
                .setRoutingStrategyId(clusRs);
        clusterDefs.add(cd);

        cd = new Cluster("test-slot1.5").adaptor(new GoodAdaptor());
        assertNotNull(cd.getAdaptor());
        clusterDefs.add(cd);

        node.setClusters(clusterDefs);

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
        final Node app = new Node("test")
                .receiver(appSer = new Object())
                .defaultRoutingStrategyId(appRs = "s")
                .clusterStatsCollectorFactoryId(appScf = "st");

        app.cluster("test-slot1").adaptor(new GoodAdaptor());
        app.cluster("test-slot2").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).destination("test-slot3");
        app.cluster("test-slot3").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).destination("test-slot4", "test-slot5");
        app.cluster("test-slot4").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        app.cluster("test-slot5").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        app.cluster("test-slot6").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).routing(clusRs = "c");
        app.cluster("test-slot1.5").adaptor(new GoodAdaptor());

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
        app.setClusters(cd); // no prototype or adaptor
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoPrototypeOrAdaptorBuilder() throws Throwable {
        final Node node = new Node("test");
        node.cluster("test-slot1");
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
        final Node node = new Node("test");
        node.cluster("test-slot1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp())).adaptor(new GoodAdaptor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNullClusterDefinition() throws Throwable {
        final Node node = new Node("test");
        node.setClusters(new Cluster("test-slot1").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
                null,
                new Cluster("test-slot2").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNoClusterDefinition() throws Throwable {
        final Node node = new Node("test");
        node.setClusters();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupCluster() throws Throwable {
        final Node app = new Node("test-tooMuchWine-needMore").setDefaultRoutingStrategyId("");
        app.setClusters(
                new Cluster("notTheSame").setAdaptor(new GoodAdaptor()),
                new Cluster("mp-stage1").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
                new Cluster("mp-stage2-dupped").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
                new Cluster("mp-stage2-dupped").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())),
                new Cluster("mp-stage3").setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp())));
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupClusterBuilder() throws Throwable {
        final Node node = new Node("test-tooMuchWine-needMore").defaultRoutingStrategyId("");
        node.cluster("notTheSame").adaptor(new GoodAdaptor());
        node.cluster("mp-stage1").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        node.cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        node.cluster("mp-stage2-dupped").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        node.cluster("mp-stage3").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        node.validate();
    }

    @Test
    public void testSimpleConfigWithKeyStore() throws Throwable {
        final Node app = new Node("test").defaultRoutingStrategyId("").receiver(new Object());
        final Cluster cd = new Cluster("test-slot");
        cd.setMessageProcessor(new MessageProcessor<GoodTestMp>(new GoodTestMp()));
        cd.setKeySource(new KeySource<Object>() {
            @Override
            public Iterable<Object> getAllPossibleKeys() {
                return null;
            }
        });
        app.setClusters(cd);
        app.validate();
    }

    @Test
    public void testSimpleConfigWithKeyStoreBuilder() throws Throwable {
        final Node node = new Node("test").defaultRoutingStrategyId("").receiver(new Object());
        node.cluster("test-slot").mp(new MessageProcessor<GoodTestMp>(new GoodTestMp()))
                .keySource(new KeySource<Object>() {
                    @Override
                    public Iterable<Object> getAllPossibleKeys() {
                        return null;
                    }
                });
        node.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStore() throws Throwable {
        final Node app = new Node("test").defaultRoutingStrategyId("");
        final Cluster cd = new Cluster("test-slot");
        cd.setAdaptor(new Adaptor() {
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
        app.setClusters(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStoreBuilder() throws Throwable {
        final Node node = new Node("test").defaultRoutingStrategyId("");
        node.cluster("test-slot").adaptor(new Adaptor() {
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
        });
        node.validate();
    }

    @Test
    public void testConfigMpWithGoodMPEvict() throws Throwable {
        final Node app = new Node("test").defaultRoutingStrategyId("").receiver(new Object());
        final Cluster cd = new Cluster("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1(final Object arg) {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        cd.setMessageProcessor(new MessageProcessor<mp>(new mp()));
        app.setClusters(cd);
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
            public boolean evict1(final Object arg) {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        final Node node = new Node("test").setDefaultRoutingStrategyId("").setReceiver(new Object());
        node.cluster("slot").mp(new MessageProcessor<mp>(new mp()));
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
