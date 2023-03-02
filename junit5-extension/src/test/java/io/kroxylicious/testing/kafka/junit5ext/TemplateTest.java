/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerConfig;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.kraftCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.zooKeeperCluster;
import static io.kroxylicious.testing.kafka.junit5ext.AbstractExtensionTest.assertSameCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(KafkaClusterExtension.class)
public class TemplateTest {

    static Stream<BrokerCluster> clusterSizes() {
        return Stream.of(
                brokerCluster(1),
                brokerCluster(3));
    }

    static AtomicReference<Map<Integer, Integer>> observedMultipleClusterSizes = new AtomicReference<>();

    @TestTemplate
    public void testMultipleClusterSizes(@DimensionMethodSource("clusterSizes") KafkaCluster cluster)
            throws Exception {
        observedMultipleClusterSizes.compareAndSet(null, new HashMap<>());
        try (var admin = Admin.create(cluster.getKafkaClientConfiguration())) {
            observedMultipleClusterSizes.get().compute(admin.describeCluster().nodes().get().size(),
                    (k, v) -> v == null ? 1 : v + 1);
        }
    }

    @AfterAll
    public static void checkMultipleClusterSizes() {
        assumeTrue(observedMultipleClusterSizes.get() != null);
        assertEquals(Map.of(1, 1, 3, 1),
                observedMultipleClusterSizes.get());
    }

    static AtomicReference<Map<Integer, Integer>> observedMultipleClusterSizesWithAdminParameters = new AtomicReference<>();

    @TestTemplate
    public void testMultipleClusterSizesWithAdminParameters(@DimensionMethodSource("clusterSizes") KafkaCluster cluster,
                                                            Admin admin)
            throws Exception {
        observedMultipleClusterSizesWithAdminParameters.compareAndSet(null, new HashMap<>());
        observedMultipleClusterSizesWithAdminParameters.get().compute(admin.describeCluster().nodes().get().size(),
                (k, v) -> v == null ? 1 : v + 1);
    }

    @AfterAll
    public static void checkMultipleClusterSizesWithAdminParameters() {
        assumeTrue(observedMultipleClusterSizesWithAdminParameters.get() != null);
        assertEquals(Map.of(1, 1, 3, 1),
                observedMultipleClusterSizesWithAdminParameters.get());
    }

    static Stream<BrokerConfig> compression() {
        return Stream.of(
                brokerConfig("compression.type", "zstd"),
                brokerConfig("compression.type", "snappy"));
    }

    static AtomicReference<Set<List<Object>>> observedCartesianProduct = new AtomicReference<>();

    @TestTemplate
    public void testCartesianProduct(@DimensionMethodSource("clusterSizes") @DimensionMethodSource("compression") KafkaCluster cluster,
                                     Admin admin)
            throws Exception {
        observedCartesianProduct.compareAndSet(null, new HashSet<>());
        assertSameCluster(cluster, admin);
        int numBrokers = admin.describeCluster().nodes().get().size();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Config configs = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        var compression = configs.get("compression.type").value();

        observedCartesianProduct.get().add(List.of(
                numBrokers,
                compression));
    }

    @AfterAll
    public static void checkCartesianProduct() {
        assumeTrue(observedCartesianProduct.get() != null);

        assertEquals(Set.of(
                List.of(1, "zstd"),
                List.of(1, "snappy"),
                List.of(3, "zstd"),
                List.of(3, "snappy")),
                observedCartesianProduct.get());
    }

    static Stream<List<Annotation>> tuples() {
        return Stream.of(
                List.of(brokerCluster(1), kraftCluster(1)),
                List.of(brokerCluster(3), kraftCluster(1)),
                List.of(brokerCluster(3), zooKeeperCluster()));
    }

    static AtomicReference<Set<List<Integer>>> observedTuples = new AtomicReference<>();

    @TestTemplate
    public void testTuples(@ConstraintsMethodSource("tuples") KafkaCluster cluster, Admin admin)
            throws Exception {
        observedTuples.compareAndSet(null, new HashSet<>());
        int numBrokers = admin.describeCluster().nodes().get().size();
        int numControllers;
        try {
            numControllers = admin.describeMetadataQuorum().quorumInfo().get().voters().size();
        }
        catch (ExecutionException e) {
            // Zookeeper-based clusters don't support this API
            if (e.getCause() instanceof UnsupportedVersionException) {
                numControllers = -1;
            }
            else {
                throw e;
            }
        }
        observedTuples.get().add(List.of(
                numBrokers,
                numControllers));
    }

    @AfterAll
    public static void checkTuples() {
        assumeTrue(observedTuples.get() != null);
        assertEquals(Set.of(
                List.of(1, 1),
                List.of(3, 1),
                List.of(3, -1)),
                observedTuples.get());
    }

    private static Stream<Version> versions() {
        return Stream.of(
                version("latest")
        // TODO: waiting for new versions support in ozangunalp repo https://github.com/ozangunalp/kafka-native/issues/21
        // version("3.3.1"),
        // version("3.2.1")
        );
    }

    static AtomicReference<Set<String>> observedVersions = new AtomicReference<>();

    @TestTemplate
    public void testVersions(@DimensionMethodSource("versions") @KRaftCluster TestcontainersKafkaCluster cluster) {
        observedVersions.compareAndSet(null, new HashSet<>());
        observedVersions.get().add(cluster.getKafkaVersion());
    }

    @AfterAll
    public static void checkVersions() {
        assumeTrue(observedVersions.get() != null);
        assertEquals(Set.of("latest-snapshot"), observedVersions.get());
    }

}
