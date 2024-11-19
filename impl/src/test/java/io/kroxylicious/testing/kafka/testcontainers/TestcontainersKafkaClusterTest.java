package io.kroxylicious.testing.kafka.testcontainers;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;

import static org.assertj.core.api.Assertions.assertThat;

class TestcontainersKafkaClusterTest {
    @Test
    void shouldAlwaysPullKafkaLatestVersionImage(TestInfo testInfo) {

        // Given
        KafkaClusterConfig config = KafkaClusterConfig.fromConstraints(List.of(), testInfo);
        final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage;
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(config)) {

            // When
            kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage).isNotNull().satisfies(perImagePullPolicy -> {
                assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).startsWith("latest");
                final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
            });
        }
    }

    @Test
    void shouldAlwaysPullKafkaLatestImage(TestInfo testInfo) {

        // Given
        KafkaClusterConfig config = KafkaClusterConfig.fromConstraints(List.of(), testInfo);
        final TestcontainersKafkaCluster.PerImagePullPolicy kafkaImage;
        try (TestcontainersKafkaCluster testcontainersKafkaCluster = new TestcontainersKafkaCluster(DockerImageName.parse("quay.io/ogunalp/kafka-native:latest"), null, config)) {

            // When
            kafkaImage = testcontainersKafkaCluster.getKafkaImage();

            // Then
            assertThat(kafkaImage).isNotNull().satisfies(perImagePullPolicy -> {
                assertThat(perImagePullPolicy.dockerImageName().getVersionPart()).startsWith("latest"); // TODO I think this should be equalTo but `overrideContainerImageTagIfNecessary` checks if the override is a number I suspect thats a bug...
                final ImagePullPolicy actualPullPolicy = perImagePullPolicy.pullPolicy();
                assertThat(actualPullPolicy).isInstanceOf(ImagePullPolicy.class); // We can't see `AlwaysPullPolicy` as it's not public
                assertThat(actualPullPolicy.shouldPull(perImagePullPolicy.dockerImageName())).isTrue();
            });
        }
    }
}
