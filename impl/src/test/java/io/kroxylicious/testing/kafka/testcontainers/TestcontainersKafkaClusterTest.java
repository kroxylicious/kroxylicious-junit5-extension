/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.Version;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static org.assertj.core.api.Assertions.assertThat;

@ClearEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_KAFKA_IMAGE_REPO)
@ClearEnvironmentVariable(key = TestcontainersKafkaCluster.APACHE_KAFKA_IMAGE_REPO)
@ClearEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_ZOOKEEPER_IMAGE_REPO)
@ClearEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_ZOOKEEPER_IMAGE_TAG)
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

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_KAFKA_IMAGE_REPO, value = "docker.io/example/kafka-native")
    @ParameterizedTest
    @MethodSource("fixedVersionsPre41")
    void shouldAllowEnvVarToControlNativeKafkaContainerRepositoryPre41(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
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
    @MethodSource("fixedVersionsPre41")
    void shouldAllowConfigToControlFixedKafkaVersionPre41(Version version) {
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

    @Test
    void shouldAllowConfigToControlFixedKafkaVersionPost41() {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion("4.1.0-rc2").build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getVersionPart()).isEqualTo("4.1.0-rc2");
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("fixedVersionsPost41")
    void post41Versions(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getVersionPart()).isEqualTo(version.value());
                    });
        }
    }

    @ParameterizedTest
    @MethodSource("fixedVersionsPost41")
    void post41VersionsShouldUseApacheKafkaNativeImagesByDefault(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getRegistry()).isEqualTo("mirror.gcr.io");
                        assertThat(pullPolicyForImage.getRepository()).isEqualTo("apache/kafka");
                    });
        }
    }

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.APACHE_KAFKA_IMAGE_REPO, value = "docker.io/example/kafka")
    @ParameterizedTest
    @MethodSource("fixedVersionsPost41")
    void post41VersionsShouldUseApacheKafkaNativeImagesWithRepoOverride(Version version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version.value()).build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            final DockerImageName kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage)
                    .isNotNull().satisfies(pullPolicyForImage -> {
                        assertThat(pullPolicyForImage.getRegistry()).isEqualTo("docker.io");
                        assertThat(pullPolicyForImage.getRepository()).isEqualTo("example/kafka");
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

    private static Stream<Version> fixedVersionsPre41() {
        return Stream.of(
                version("4.0.0"),
                version("3.9.0"),
                version("3.8.0"),
                version("3.7.0"),
                version("3.6.0"),
                version("3.5.1"),
                version("3.4.0"),
                version("3.2.3"),
                version("3.1.2"));
    }

    private static Stream<Version> fixedVersionsPost41() {
        return Stream.of(version("4.1.0"));
    }

    private static Stream<Version> floatingVersions() {
        return Stream.of(
                version(Version.LATEST_RELEASE));
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

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_ZOOKEEPER_IMAGE_REPO, value = "docker.io/example/native-zookeeper")
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

    @SetEnvironmentVariable(key = TestcontainersKafkaCluster.LEGACY_ZOOKEEPER_IMAGE_TAG, value = "latest-zookeeper-3.7.0")
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

    @ParameterizedTest
    @CsvSource(value = { "4.1.0", "4.0.0", "3.9.0", "3.8.0" })
    void scramUsersCreated(String version) {
        KafkaClusterConfig config = clusterConfigBuilder.kafkaVersion(version).securityProtocol("SASL_PLAINTEXT").saslMechanism("SCRAM-SHA-256")
                .user("admin", "admin-secret").build();
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {
            testcontainersKafkaCluster.start();
            Map<String, Object> kafkaClientConfiguration = testcontainersKafkaCluster.getKafkaClientConfiguration();
            assertThat(kafkaClientConfiguration).containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            assertThat(kafkaClientConfiguration).containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            kafkaClientConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            kafkaClientConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            try (var producer = CloseableProducer.create(kafkaClientConfiguration)) {
                Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("topic", "key", "value"));
                RecordMetadata recordMetadata = metadataFuture.get(10, TimeUnit.SECONDS);
                assertThat(recordMetadata).isNotNull();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
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
