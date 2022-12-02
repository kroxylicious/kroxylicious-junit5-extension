/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.common.SaslPlainAuth;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.common.User;
import io.kroxylicious.testing.kafka.common.ZooKeeperCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import kafka.server.KafkaConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaClusterExtension.class)
public class ParameterExtensionTest extends AbstractExtensionTest {

    @Test
    public void clusterParameter(@BrokerCluster(numBrokers = 2) KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(cluster.getKafkaClientConfiguration());
        assertEquals(2, dc.nodes().get().size());
        assertEquals(cluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(InVMKafkaCluster.class, cluster);
    }

    @Test
    public void brokerConfigs(@BrokerConfig(name = "compression.type", value = "zstd") @BrokerConfig(name = "delete.topic.enable", value = "false") KafkaCluster clusterWithConfigs,
                              Admin admin)
            throws ExecutionException, InterruptedException {
        assertSameCluster(clusterWithConfigs, admin);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Config configs = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        assertEquals("zstd", configs.get("compression.type").value());
        assertEquals("false", configs.get("delete.topic.enable").value());
    }

    @Test
    public void clusterAndAdminParameter(@BrokerCluster(numBrokers = 2) KafkaCluster cluster,
                                         Admin admin)
            throws ExecutionException, InterruptedException {
        var dc = assertSameCluster(cluster, admin);
        assertEquals(2, dc.nodes().get().size());
        var cbc = assertInstanceOf(InVMKafkaCluster.class, cluster);
    }

    @Test
    public void twoAnonClusterParameter(
                                        @BrokerCluster(numBrokers = 1) KafkaCluster cluster1,
                                        @BrokerCluster(numBrokers = 2) KafkaCluster cluster2)
            throws ExecutionException, InterruptedException {
        assertNotEquals(cluster1.getClusterId(), cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals(cluster1.getClusterId(), dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals(cluster2.getClusterId(), dc2.clusterId().get());
    }

    // @Name is not required here because there's no ambiguity
    @Test
    public void twoDefinedClusterParameter(
                                           @BrokerCluster(numBrokers = 1) KafkaCluster cluster1,
                                           @BrokerCluster(numBrokers = 2) KafkaCluster cluster2)
            throws ExecutionException, InterruptedException {
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
    }

    @Test
    public void twoDefinedClusterParameterAndAdmin(
                                                   @BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster clusterA,
                                                   @BrokerCluster(numBrokers = 2) @Name("B") KafkaCluster clusterB,
                                                   @Name("B") Admin adminB,
                                                   @Name("A") Admin adminA)
            throws ExecutionException, InterruptedException {
        var dc1 = describeCluster(clusterA.getKafkaClientConfiguration());
        assertSameCluster(clusterA, adminA);
        assertEquals(1, dc1.nodes().get().size());
        assertEquals(1, describeCluster(adminA).nodes().get().size());
        var dc2 = describeCluster(clusterB.getKafkaClientConfiguration());
        assertSameCluster(clusterB, adminB);
        assertEquals(2, dc2.nodes().get().size());
        assertEquals(2, describeCluster(adminB).nodes().get().size());
    }

    // multiple clients connected to the same cluster (e.g. different users)
    @Test
    public void twoClusterParameterAndTwoAdmin(
                                               @BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster cluster1,
                                               @Name("A") Admin admin1,
                                               @Name("A") Admin admin2)
            throws ExecutionException, InterruptedException {
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster1, admin2);
        assertNotSame(admin1, admin2);
    }

    @Test
    public void zkBasedClusterParameter(@BrokerCluster @ZooKeeperCluster KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(cluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertNull(cluster.getClusterId(),
                "KafkaCluster.getClusterId() should be null for ZK-based clusters");
    }

    @Test
    public void kraftBasedClusterParameter(@BrokerCluster @KRaftCluster KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(cluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
    }

    @Test
    public void saslPlainAuthenticatingClusterParameterOneUser(
                                                               @BrokerCluster @SaslPlainAuth @User(user = "alice", password = "foo") KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(cluster.getKafkaClientConfiguration("alice", "foo"));
        assertEquals(1, dc.nodes().get().size());
        assertEquals(cluster.getClusterId(), dc.clusterId().get());

        var ee = assertThrows(ExecutionException.class, () -> describeCluster(cluster.getKafkaClientConfiguration("alice", "FOO")),
                "Expect bad password to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());

        ee = assertThrows(ExecutionException.class, () -> describeCluster(cluster.getKafkaClientConfiguration("eve", "quux")),
                "Expect unknown user to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());
    }

    @Test
    public void saslPlainAuthenticatingClusterParameterTwoUsers(
                                                                @BrokerCluster @SaslPlainAuth @User(user = "alice", password = "foo") @User(user = "bob", password = "bar") KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(cluster.getKafkaClientConfiguration("alice", "foo"));
        assertEquals(1, dc.nodes().get().size());
        assertEquals(cluster.getClusterId(), dc.clusterId().get());

        dc = describeCluster(cluster.getKafkaClientConfiguration("bob", "bar"));
        assertEquals(cluster.getClusterId(), dc.clusterId().get());

        var ee = assertThrows(ExecutionException.class, () -> describeCluster(cluster.getKafkaClientConfiguration("bob", "baz")),
                "Expect bad password to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());

        ee = assertThrows(ExecutionException.class, () -> describeCluster(cluster.getKafkaClientConfiguration("eve", "quux")),
                "Expect unknown user to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());
    }

    @Test
    public void tlsClusterParameter(
                                    @Tls @BrokerCluster(numBrokers = 1) KafkaCluster cluster,
                                    Admin admin)
            throws ExecutionException, InterruptedException {
        String bootstrapServer = cluster.getBootstrapServers();
        assertFalse(bootstrapServer.contains(","), "expect a single bootstrap server");
        var listenerPattern = Pattern.compile("(?<listenerName>[a-zA-Z]+)://" + Pattern.quote(bootstrapServer));
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Config brokerConfigs = admin.describeConfigs(List.of(broker)).all().get().get(broker);
        String advertisedListener = brokerConfigs.get(KafkaConfig.AdvertisedListenersProp()).value();
        // e.g. advertisedListener = "EXTERNAL://localhost:37565,INTERNAL://localhost:35173"
        var matcher = listenerPattern.matcher(advertisedListener);
        assertTrue(matcher.find(),
                "Expected '" + advertisedListener + "' to contain a match for " + listenerPattern.pattern());
        var listenerName = matcher.group("listenerName");
        String protocolMap = brokerConfigs.get(KafkaConfig.ListenerSecurityProtocolMapProp()).value();
        assertTrue(protocolMap.contains(listenerName + ":SSL"),
                "Expected '" + protocolMap + "' to contain " + listenerName + ":SSL");

    }

}
