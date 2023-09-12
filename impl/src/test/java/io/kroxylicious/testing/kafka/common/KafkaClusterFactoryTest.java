/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaClusterFactoryTest {

    @SuppressWarnings("resource")
    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = { "KRAFT_COMBINED", "KRAFT_SEPARATE" })
    void shouldThrowInContainerModeWithControllerOnlyNodes(MetadataMode metadataMode) {
        // Due to https://github.com/ozangunalp/kafka-native/issues/88 we can't support controller only nodes, so we need to fail fast
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.CONTAINER)
                .metadataMode(metadataMode)
                .brokersNum(1)
                .kraftControllers(2)
                .build();

        // When
        assertThrows(IllegalStateException.class, () -> KafkaClusterFactory.create(kafkaClusterConfig));
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = { "KRAFT_COMBINED", "KRAFT_SEPARATE" })
    void shouldCreateInstanceInVMModeWithControllerOnlyNodes(MetadataMode metadataMode) {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.IN_VM)
                .metadataMode(metadataMode)
                .brokersNum(1)
                .kraftControllers(2)
                .build();

        // When
        try (var kafkaCluster = KafkaClusterFactory.create(kafkaClusterConfig)) {
            assertThat(kafkaCluster).isNotNull();
        }
        catch (Exception e) {
            fail("Failed to create KafkaCluster: " + e.getMessage(), e);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = { "KRAFT_COMBINED", "KRAFT_SEPARATE" })
    void shouldCreateInstanceInContainerModeWithoutControllerOnlyNodes(MetadataMode metadataMode) {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.IN_VM).metadataMode(metadataMode)
                .brokersNum(3).kraftControllers(2).build();

        // When
        try (var kafkaCluster = KafkaClusterFactory.create(kafkaClusterConfig)) {
            assertThat(kafkaCluster).isNotNull();
        }
        catch (Exception e) {
            fail("Failed to create KafkaCluster: " + e.getMessage(), e);
        }
    }

}
