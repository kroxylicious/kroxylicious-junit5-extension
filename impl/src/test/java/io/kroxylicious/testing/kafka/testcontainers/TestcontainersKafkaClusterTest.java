/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.testcontainers;

import org.junit.jupiter.api.Test;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterExecutionMode;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestcontainersKafkaClusterTest {

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
        assertThrows(IllegalStateException.class, () -> new TestcontainersKafkaCluster(kafkaClusterConfig));
    }
}
