/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaClusterFactoryTest {

    @SuppressWarnings("resource")
    @Test
    void shouldThrowInContainerModeWithControllerOnlyNodes() {
        // Due to https://github.com/ozangunalp/kafka-native/issues/88 we can't support controller only nodes, so we need to fail fast
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.CONTAINER)
                .kraftMode(true)
                .brokersNum(1)
                .kraftControllers(2)
                .build();

        // When
        assertThrows(IllegalStateException.class, () -> KafkaClusterFactory.create(kafkaClusterConfig));
    }

    @Test
    void shouldCreateInstanceInVMModeWithControllerOnlyNodes() {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.IN_VM)
                .kraftMode(true)
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

    @Test
    void shouldCreateInstanceInContainerModeWithoutControllerOnlyNodes() {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.IN_VM).kraftMode(true)
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
