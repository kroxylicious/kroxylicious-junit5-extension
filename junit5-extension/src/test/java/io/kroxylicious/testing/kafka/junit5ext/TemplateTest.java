/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.common.Utils;
import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerConfig;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.kraftCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.zooKeeperCluster;
import static io.kroxylicious.testing.kafka.junit5ext.AbstractExtensionTest.assertSameCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class)
public class TemplateTest {

    static Stream<BrokerCluster> clusterSizes() {
        return Stream.of(
                brokerCluster(1),
                brokerCluster(3));
    }

    @TestTemplate
    public void testMultipleClusterSizes(
                                         @DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(cluster.getKafkaClientConfiguration())) {
            assertEquals(admin.describeCluster().nodes().get().size(), cluster.getNumOfBrokers());
        }
    }

    @TestTemplate
    public void testMultipleClusterSizesWithAdminParameters(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster,
                                                            Admin admin)
            throws ExecutionException, InterruptedException {
        assertEquals(admin.describeCluster().nodes().get().size(), cluster.getNumOfBrokers());
    }

    static Set<List<Object>> observedCartesianProduct = new HashSet<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class CartesianProduct {
        static Stream<BrokerConfig> compression() {
            return Stream.of(
                    brokerConfig("compression.type", "zstd"),
                    brokerConfig("compression.type", "snappy"));
        }

        @TestTemplate
        public void testCartesianProduct(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) @DimensionMethodSource(value = "compression") KafkaCluster cluster,
                                         Admin admin)
                throws ExecutionException, InterruptedException {
            // Given
            assertSameCluster(cluster, admin);
            Utils.awaitExpectedBrokerCountInClusterViaTopic(cluster.getKafkaClientConfiguration(), 30, TimeUnit.SECONDS, cluster.getNumOfBrokers());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");

            // When
            Config configs = admin.describeConfigs(List.of(resource)).all().get().get(resource);

            // Then
            int actualClusterSize = admin.describeCluster().nodes().get().size();
            assertThat(actualClusterSize).as("Expected cluster to have %s nodes", cluster.getNumOfBrokers()).isEqualTo(cluster.getNumOfBrokers());

            var compression = configs.get("compression.type").value();
            observedCartesianProduct.add(List.of(
                    cluster.getNumOfBrokers(),
                    compression));
        }

        @AfterAll
        public void afterAll() {
            // We are basically asserting that the test was invoked with all the expected combinations of parameters
            assertThat(observedCartesianProduct).containsExactlyInAnyOrder(
                    List.of(1, "zstd"),
                    List.of(1, "snappy"),
                    List.of(3, "zstd"),
                    List.of(3, "snappy"));
        }
    }

    static Stream<List<Annotation>> tuples() {
        return Stream.of(
                List.of(brokerCluster(1), kraftCluster(1)),
                List.of(brokerCluster(3), kraftCluster(1)),
                List.of(brokerCluster(3), zooKeeperCluster()));
    }

    static Set<List<Integer>> observedTuples = new HashSet<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class Tuples {

        @TestTemplate
        public void testTuples(@ConstraintsMethodSource(value = "tuples", clazz = TemplateTest.class) KafkaCluster cluster,
                               Admin admin)
                throws ExecutionException, InterruptedException {
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
            observedTuples.add(List.of(
                    numBrokers,
                    numControllers));
        }

        @AfterAll
        public void afterAll() {
            assertEquals(Set.of(
                    List.of(1, 1),
                    List.of(3, 1),
                    List.of(3, -1)),
                    observedTuples);
        }
    }

    private static Stream<Version> versions() {
        return Stream.of(
                version("3.6.0"),
                version("3.5.1"),
                version("3.4.0"),
                version("3.2.3"),
                version("3.1.2"));
    }

    static Set<String> observedVersions = new HashSet<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class Versions {

        @TestTemplate
        public void testVersions(@DimensionMethodSource(value = "versions", clazz = TemplateTest.class) @KRaftCluster TestcontainersKafkaCluster cluster) {
            observedVersions.add(cluster.getKafkaVersion());
        }

        @AfterAll
        public void afterAll() {
            assertEquals(versions().map(Version::value).map("latest-kafka-%s"::formatted).collect(Collectors.toSet()), observedVersions);
        }
    }
}
