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
import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerConfig;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.kraftCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.zooKeeperCluster;
import static io.kroxylicious.testing.kafka.junit5ext.AbstractExtensionTest.assertSameCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class)
public class TemplateTest {

    static Stream<BrokerCluster> clusterSizes() {
        return Stream.of(
                brokerCluster(1),
                brokerCluster(3));
    }

    static Map<Integer, Integer> observedMultipleClusterSizes = new HashMap<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class MultipleClusterSizes {
        @TestTemplate
        public void testMultipleClusterSizes(
                                             @DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster)
                throws ExecutionException, InterruptedException {
            try (var admin = Admin.create(cluster.getKafkaClientConfiguration())) {
                observedMultipleClusterSizes.compute(admin.describeCluster().nodes().get().size(),
                        (k, v) -> v == null ? 1 : v + 1);
            }
        }

        @AfterAll
        public void afterAll() {
            assertEquals(Map.of(1, 1, 3, 1),
                    observedMultipleClusterSizes);
        }
    }

    static Map<Integer, Integer> observedMultipleClusterSizesWithAdminParameters = new HashMap<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class MultipleClusterSizesWithAdminParameters {
        @TestTemplate
        public void testMultipleClusterSizesWithAdminParameters(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster,
                                                                Admin admin)
                throws ExecutionException, InterruptedException {
            observedMultipleClusterSizesWithAdminParameters.compute(admin.describeCluster().nodes().get().size(),
                    (k, v) -> v == null ? 1 : v + 1);
        }

        @AfterAll
        public void afterAll() {
            assertEquals(Map.of(1, 1, 3, 1),
                    observedMultipleClusterSizesWithAdminParameters);
        }
    }

    static Stream<BrokerConfig> compression() {
        return Stream.of(
                brokerConfig("compression.type", "zstd"),
                brokerConfig("compression.type", "snappy"));
    }

    static Set<List<Object>> observedCartesianProduct = new HashSet<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class CartesianProduct {
        @TestTemplate
        public void testCartesianProduct(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) @DimensionMethodSource(value = "compression", clazz = TemplateTest.class) KafkaCluster cluster,
                                         Admin admin)
                throws ExecutionException, InterruptedException {
            assertSameCluster(cluster, admin);
            int numBrokers = admin.describeCluster().nodes().get().size();
            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Config configs = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            var compression = configs.get("compression.type").value();

            observedCartesianProduct.add(List.of(
                    numBrokers,
                    compression));
        }

        @AfterAll
        public void afterAll() throws Exception {
            assertEquals(Set.of(
                    List.of(1, "zstd"),
                    List.of(1, "snappy"),
                    List.of(3, "zstd"),
                    List.of(3, "snappy")),
                    observedCartesianProduct);

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
                version("latest"),
                version("3.4.0"),
                version("3.2.3"),
                version("3.1.2")
        );
    }

    static Set<String> observedVersions = new HashSet<>();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public class Versions {

        @TestTemplate
        public void testVersions(@DimensionMethodSource(value = "versions", clazz = TemplateTest.class) @KRaftCluster TestcontainersKafkaCluster cluster)
                throws Exception {
            observedVersions.add(cluster.getKafkaVersion());
        }

        @AfterAll
        public void afterAll() {
            assertEquals(Set.of("latest-kafka-3.1.2",
                                "latest-kafka-3.2.3",
                                "latest-kafka-3.4.0",
                                "latest"), observedVersions);
        }
    }
}
