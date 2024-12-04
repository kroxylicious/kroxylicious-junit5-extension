/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

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
    void shouldUseFloatingTagPullPolicyForKafkaImage() {

        // Given
        KafkaClusterConfig config = clusterConfigBuilder.build();

        // When
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // Then
            assertCustomPullPolicy(testcontainersKafkaCluster);
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.KAFKA_IMAGE_REPO, value = "docker.io/example/kafka-native")
    @Test
    void shouldAllowEnvVarToControlKafkaContainerRepository() {
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull()
                    .satisfies(imageName -> {
                        assertThat(imageName.getRegistry()).isEqualTo("docker.io");
                        assertThat(imageName.getRepository()).isEqualTo("example/kafka-native");
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.KAFKA_IMAGE_TAG, value = "latest-kafka-3.7.0")
    @Test
    void shouldAllowEnvVarToControlKafkaVersion() {
        KafkaClusterConfig config = clusterConfigBuilder.build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull()
                    .satisfies(imageName -> {
                        // this works because the classpath will be 3.9.0 or greater so we are forcing a fallback
                        assertThat(imageName.getVersionPart()).isEqualTo("latest-kafka-3.7.0");
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("fixedVersions")
    void shouldAllowConfigToControlFixedKafkaVersion(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getVersionPart()).isEqualTo("latest-kafka-" + version.value());
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("floatingVersions")
    void shouldAllowConfigToControlFloatingKafkaVersion(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull()
                    .satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getVersionPart()).isEqualTo(version.value());
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
    void shouldUseCustomPullPolicyForZookeeperImage() {
        // Given
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        // When
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // Then
            assertCustomPullPolicy(testcontainersKafkaCluster);
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_REPO, value = "docker.io/example/native-zookeeper")
    @Test
    void shouldAllowEnvVarToControlZookeeperImage() {
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull()
                    .satisfies(imageName -> {
                        assertThat(imageName.getRegistry()).isEqualTo("docker.io");
                        assertThat(imageName.getRepository()).isEqualTo("example/native-zookeeper");
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_TAG, value = "latest-zookeeper-3.7.0")
    @Test
    void shouldAllowEnvVarToControlZookeeperVersion() {
        KafkaClusterConfig config = clusterConfigBuilder.kraftMode(false).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName zookeeperImage = testcontainersKafkaCluster.getZookeeperImage();

            // Then
            assertThat(zookeeperImage)
                    .isNotNull()
                    .satisfies(pullPolicyForImage -> {
                        // this works because the classpath will be 3.9.0 or greater so we are forcing a fallback
                        assertThat(pullPolicyForImage.getVersionPart()).isEqualTo("latest-zookeeper-3.7.0");
                    });
        }
    }

    private static void assertCustomPullPolicy(TestcontainersKafkaCluster testcontainersKafkaCluster) {
        assertThat(testcontainersKafkaCluster.allContainers())
                .isNotEmpty()
                .allSatisfy((GenericContainer<?> container) -> {
                    assertThat(container).extracting("image")
                            .asInstanceOf(InstanceOfAssertFactories.type(RemoteDockerImage.class))
                            .extracting("imagePullPolicy")
                            .isInstanceOf(FloatingTagPullPolicy.class);
                });
    }
}
