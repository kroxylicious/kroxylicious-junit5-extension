/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.testcontainers.images.ImagePullPolicy;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.Version;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static org.assertj.core.api.Assertions.assertThat;

class TestcontainersKafkaClusterTest {

    private KafkaClusterConfig.KafkaClusterConfigBuilder clusterConfigBuilder;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        clusterConfigBuilder = KafkaClusterConfig.builder();
        clusterConfigBuilder.testInfo(testInfo);
    }

    @Test
    void shouldAlwaysPullKafkaLatestVersionImage() {

        // Given
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).startsWith("latest-kafka-");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.KAFKA_IMAGE_TAG, value = "latest-snapshot")
    @Test
    void shouldAlwaysPullKafkaLatestImage() {

        // Given
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo("latest-snapshot");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.KAFKA_IMAGE_REPO, value = "docker.io/example/kafka-native")
    @Test
    void shouldAllowEnvVarToControlKafkaContainerRepository() {
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getRegistry()).isEqualTo("docker.io");
                        assertThat(perImagePullPolicy.dockerImageName().getRepository()).isEqualTo("example/kafka-native");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.KAFKA_IMAGE_TAG, value = "latest-kafka-3.7.0")
    @Test
    void shouldAllowEnvVarToControlKafkaVersion() {
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        // this works because the classpath will be 3.9.0 or greater so we are forcing a fallback
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo("latest-kafka-3.7.0");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("fixedVersions")
    void shouldAllowConfigToControlFixedKafkaVersion(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo("latest-kafka-" + version.value());
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("floatingVersions")
    void shouldAllowConfigToControlFloatingKafkaVersion(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo(version.value());
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    private static Stream<Version> fixedVersions() {
        return Stream.of(
                version("3.9.0"),
                version("3.8.0"),
                version("3.7.0"),
                version("3.6.0"),
                version("3.5.1"),
                version("3.4.0"),
                version("3.2.3"),
                version("3.1.2"));
    }

    private static Stream<Version> floatingVersions() {
        return Stream.of(
                version(Version.LATEST_RELEASE),
                version(Version.LATEST_SNAPSHOT));
    }

    @Test
    void shouldAlwaysPullZookeeperLatestVersionImage() {
        // Given
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).startsWith("latest");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_TAG, value = "latest-snapshot")
    @Test
    void shouldAlwaysPullZookeeperLatestImage() {
        // Given
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo("latest-snapshot");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_REPO, value = "docker.io/example/native-zookeeper")
    @Test
    void shouldAllowEnvVarToControlZookeeperImage() {
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        assertThat(perImagePullPolicy.dockerImageName().getRegistry()).isEqualTo("docker.io");
                        assertThat(perImagePullPolicy.dockerImageName().getRepository()).isEqualTo("example/native-zookeeper");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_TAG, value = "latest-zookeeper-3.7.0")
    @Test
    void shouldAllowEnvVarToControlZookeeperVersion() {
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final TestcontainersKafkaCluster.PerImagePullPolicy zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull().satisfies(perImagePullPolicy -> {
                        // this works because the classpath will be 3.9.0 or greater so we are forcing a fallback
                        assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).isEqualTo("latest-zookeeper-3.7.0");
                        final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                        assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                        assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
                    });
        }
    }
}
