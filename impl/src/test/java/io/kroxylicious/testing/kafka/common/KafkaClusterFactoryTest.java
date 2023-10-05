/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClusterFactoryTest {

    @SuppressWarnings("resource")
    @Test
    void shouldCreateInstanceInContainerModeWithControllerOnlyNodes() throws Exception {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.CONTAINER)
                .kraftMode(true)
                .brokersNum(1)
                .kraftControllers(2)
                .build();

        // When
        try (var kafkaCluster = KafkaClusterFactory.create(kafkaClusterConfig)) {
            assertThat(kafkaCluster).isNotNull();
        }
    }

    @Test
    void shouldCreateInstanceInVMModeWithControllerOnlyNodes() throws Exception {
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
    }

    @Test
    void shouldCreateInstanceInContainerModeWithoutControllerOnlyNodes() throws Exception {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = KafkaClusterConfig.builder()
                .execMode(KafkaClusterExecutionMode.IN_VM).kraftMode(true)
                .brokersNum(3).kraftControllers(2).build();

        // When
        try (var kafkaCluster = KafkaClusterFactory.create(kafkaClusterConfig)) {
            assertThat(kafkaCluster).isNotNull();
        }
    }

}
