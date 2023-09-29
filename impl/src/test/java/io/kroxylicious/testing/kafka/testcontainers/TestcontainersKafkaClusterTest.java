/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.testcontainers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterExecutionMode;
import io.kroxylicious.testing.kafka.common.MetadataMode;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestcontainersKafkaClusterTest {

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
        assertThrows(IllegalStateException.class, () -> new TestcontainersKafkaCluster(kafkaClusterConfig));
    }
}
