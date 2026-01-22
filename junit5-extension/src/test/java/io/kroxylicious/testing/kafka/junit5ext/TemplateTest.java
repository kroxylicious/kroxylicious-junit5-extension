/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import edu.umd.cs.findbugs.annotations.NonNull;

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
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class TemplateTest {

    private static final boolean ZOOKEEPER_AVAILABLE = AbstractExtensionTest.zookeeperAvailable();

    @SuppressWarnings("unused")
    static Stream<BrokerCluster> clusterSizes() {
        return Stream.of(
                brokerCluster(1),
                brokerCluster(3));
    }

    @TestTemplate
    void testMultipleClusterSizes(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(cluster.getKafkaClientConfiguration())) {
            assertThat(admin.describeCluster().nodes().get()).hasSize(cluster.getNumOfBrokers());
        }
    }

    @TestTemplate
    void testMultipleClusterSizesWithAdminParameters(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) KafkaCluster cluster,
                                                     Admin admin)
            throws ExecutionException, InterruptedException {
        assertThat(admin.describeCluster().nodes().get()).hasSize(cluster.getNumOfBrokers());
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class CartesianProduct {

        private final Set<List<Object>> observedCartesianProduct = new HashSet<>();

        @SuppressWarnings("unused")
        static Stream<BrokerConfig> compression() {
            return Stream.of(
                    brokerConfig("compression.type", "zstd"),
                    brokerConfig("compression.type", "snappy"));
        }

        @TestTemplate
        void testCartesianProduct(@DimensionMethodSource(value = "clusterSizes", clazz = TemplateTest.class) @DimensionMethodSource(value = "compression") KafkaCluster cluster,
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
        void afterAll() {
            // We are basically asserting that the test was invoked with all the expected combinations of parameters
            assertThat(observedCartesianProduct).containsExactlyInAnyOrder(
                    List.of(1, "zstd"),
                    List.of(1, "snappy"),
                    List.of(3, "zstd"),
                    List.of(3, "snappy"));
        }
    }

    @SuppressWarnings("unused")
    static Stream<List<Annotation>> tuples() {
        var tuples = new ArrayList<>(List.of(
                List.of(brokerCluster(1), kraftCluster(1)),
                List.of(brokerCluster(3), kraftCluster(1))));
        if (ZOOKEEPER_AVAILABLE) {
            tuples.add(List.of(brokerCluster(3), zooKeeperCluster()));
        }
        return tuples.stream();

    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Tuples {
        private final Set<List<Integer>> observedTuples = new HashSet<>();

        @TestTemplate
        void testTuples(@ConstraintsMethodSource(value = "tuples", clazz = TemplateTest.class) KafkaCluster cluster,
                        Admin admin) {

            // Workaround for https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/391
            int numBrokers = await("countBrokers")
                    .until(() -> admin.describeCluster().nodes().get().size(), brokers -> brokers > 0);
            int numControllers = await("countControllers")
                    .until(() -> {

                        try {
                            return admin.describeMetadataQuorum().quorumInfo().get().voters().size();
                        }
                        catch (ExecutionException e) {
                            // Zookeeper-based clusters don't support this API
                            if (e.getCause() instanceof UnsupportedVersionException) {
                                return -1;
                            }
                            else {
                                throw e;
                            }
                        }
                    }, controllers -> controllers == -1 || controllers > 0);
            observedTuples.add(List.of(
                    numBrokers,
                    numControllers));
        }

        @AfterAll
        void afterAll() {
            var expected = new HashSet<>(Set.of(
                    List.of(1, 1),
                    List.of(3, 1)));
            if (ZOOKEEPER_AVAILABLE) {
                expected.add(List.of(3, -1));
            }
            assertThat(observedTuples)
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static Stream<Version> versions() {
        return Stream.of(
                version(Version.LATEST_RELEASE),
                version("3.9.0"),
                version("3.8.0"),
                version("3.7.0"),
                version("3.6.0"),
                version("3.5.1"),
                version("3.4.0"),
                version("3.2.3"),
                version("3.1.2"));
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Versions {

        private final Set<String> observedVersions = new HashSet<>();

        @TestTemplate
        void testVersions(@DimensionMethodSource(value = "versions", clazz = TemplateTest.class) @KRaftCluster TestcontainersKafkaCluster cluster) {
            observedVersions.add(cluster.getKafkaVersion());
        }

        @AfterAll
        void afterAll() {
            var expected = versions().map(Version::value).collect(Collectors.toSet());
            assertThat(observedVersions)
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class CooperationWithAnotherTestTemplateInvocationContextProvider {
        private final AtomicInteger observedInvocations = new AtomicInteger();
        private final Set<MyTestParam> observedResolvedParams = new HashSet<>();

        @TestTemplate
        @ExtendWith(MyTestTemplateInvocationContextProvider.class)
        void invocation(KafkaCluster cluster) {
            assertThat(cluster).isNotNull();
            observedInvocations.incrementAndGet();
        }

        @TestTemplate
        @ExtendWith(MyTestTemplateInvocationContextProvider.class)
        void invocationWithResolvedParameter(KafkaCluster cluster, MyTestParam testParam) {
            assertThat(cluster).isNotNull();
            observedResolvedParams.add(testParam);
        }

        @AfterAll
        void afterAll() {
            assertThat(observedInvocations).hasValue(2);
            assertThat(observedResolvedParams).containsExactly(new MyTestParam("one"), new MyTestParam("two"));
        }

    }

    private record MyTestParam(String value) {}

    private static class MyTestTemplateInvocationContextProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return Stream.of(
                    getTestTemplateInvocationContext("one"),
                    getTestTemplateInvocationContext("two"));
        }

        @NonNull
        private TestTemplateInvocationContext getTestTemplateInvocationContext(String value) {
            return new TestTemplateInvocationContext() {
                @Override
                public String getDisplayName(int invocationIndex) {
                    return value;
                }

                @Override
                public List<Extension> getAdditionalExtensions() {
                    return List.of(
                            new TypeBasedParameterResolver<MyTestParam>() {

                                @Override
                                public MyTestParam resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                                        throws ParameterResolutionException {
                                    return new MyTestParam(value);
                                }
                            });
                }

            };
        }
    }

}
