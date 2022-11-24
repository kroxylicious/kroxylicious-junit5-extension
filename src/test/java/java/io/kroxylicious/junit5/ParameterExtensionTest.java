/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.cluster.InVMKafkaCluster;
import io.kroxylicious.cluster.KafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    public void twoAnonClusterParameterAndAdmin(
                                                @BrokerCluster(numBrokers = 1) KafkaCluster cluster1,
                                                Admin admin1,
                                                @BrokerCluster(numBrokers = 2) KafkaCluster cluster2,
                                                Admin admin2)
            throws ExecutionException, InterruptedException {
        assertNotEquals(cluster1.getClusterId(), cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals(cluster1.getClusterId(), dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals(cluster2.getClusterId(), dc2.clusterId().get());

        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster2, admin2);
    }

    @Test
    public void twoDefinedClusterParameter(
                                           @BrokerCluster(numBrokers = 1) @ClusterId("S-1-7BIVSNyYIjOh7xVySQ") KafkaCluster cluster1,
                                           @BrokerCluster(numBrokers = 2) @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") KafkaCluster cluster2)
            throws ExecutionException, InterruptedException {
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", cluster1.getClusterId());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", dc2.clusterId().get());
    }

    @Test
    public void twoDefinedClusterParameterAndAdmin(
                                                   @BrokerCluster(numBrokers = 1) @ClusterId("S-1-7BIVSNyYIjOh7xVySQ") KafkaCluster cluster1,
                                                   @ClusterId("S-1-7BIVSNyYIjOh7xVySQ") Admin admin1,
                                                   @BrokerCluster(numBrokers = 2) @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") KafkaCluster cluster2,
                                                   @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") Admin admin2)
            throws ExecutionException, InterruptedException {
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", cluster1.getClusterId());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", dc2.clusterId().get());

        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster2, admin2);
    }

    @Test
    public void twoDefinedClusterParameterAndAdmin2(
                                                    @BrokerCluster(numBrokers = 1) @ClusterId("S-1-7BIVSNyYIjOh7xVySQ") KafkaCluster cluster1,
                                                    @BrokerCluster(numBrokers = 2) @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") KafkaCluster cluster2,
                                                    @ClusterId("S-1-7BIVSNyYIjOh7xVySQ") Admin admin1,
                                                    @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") Admin admin2)
            throws ExecutionException, InterruptedException {
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", cluster1.getClusterId());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("S-1-7BIVSNyYIjOh7xVySQ", dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals("0yG1FxVOTGWY7bcqV1UAzg", dc2.clusterId().get());

        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster2, admin2);
    }

    @Test
    public void twoPrefixedClusterParameter(
                                            @BrokerCluster(numBrokers = 1) @ClusterId("A") KafkaCluster cluster1,
                                            @BrokerCluster(numBrokers = 2) @ClusterId("B") KafkaCluster cluster2)
            throws ExecutionException, InterruptedException {
        assertEquals("A00000000000000000000w", cluster1.getClusterId());
        assertEquals("B00000000000000000000w", cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("A00000000000000000000w", dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals("B00000000000000000000w", dc2.clusterId().get());
    }

    @Test
    public void twoPrefixedClusterParameter(
                                            @BrokerCluster(numBrokers = 1) @ClusterId("A") KafkaCluster cluster1,
                                            @BrokerCluster(numBrokers = 2) @ClusterId("B") KafkaCluster cluster2,
                                            @ClusterId("B") Admin admin2,
                                            @ClusterId("A") Admin admin1)
            throws ExecutionException, InterruptedException {
        assertEquals("A00000000000000000000w", cluster1.getClusterId());
        assertEquals("B00000000000000000000w", cluster2.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("A00000000000000000000w", dc1.clusterId().get());
        var dc2 = describeCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals("B00000000000000000000w", dc2.clusterId().get());

        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster2, admin2);
    }

    // multiple clients connected to the same cluster (e.g. different users)
    @Test
    public void twoClusterParameterAndTwoAdmin(
                                               @BrokerCluster(numBrokers = 1) @ClusterId("A") KafkaCluster cluster1,
                                               @ClusterId("A") Admin admin1,
                                               @ClusterId("A") Admin admin2)
            throws ExecutionException, InterruptedException {
        assertEquals("A00000000000000000000w", cluster1.getClusterId());
        var dc1 = describeCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals("A00000000000000000000w", dc1.clusterId().get());
        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster1, admin2);
        assertNotSame(admin1, admin2);
    }

    // throw if two clusters declared with same cluster id
    @Test
    @Disabled
    public void twoClusterParametersWithSameId(
                                               @BrokerCluster(numBrokers = 1) @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") KafkaCluster cluster1,
                                               @BrokerCluster(numBrokers = 2) @ClusterId("0yG1FxVOTGWY7bcqV1UAzg") KafkaCluster cluster2) {
        assertSame(cluster1, cluster2);
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
    public void saslPlainAuthenticatingClusterParameter(
                                                        @BrokerCluster @SaslPlainAuth({
                                                                @SaslPlainAuth.UserPassword(user = "alice", password = "foo"),
                                                                @SaslPlainAuth.UserPassword(user = "bob", password = "bar")
                                                        }) KafkaCluster cluster)
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
    public void impossibleConstraint(
                                     @BrokerCluster(numBrokers = 1) @ImpossibleConstraint KafkaCluster cluster) {

    }

}
