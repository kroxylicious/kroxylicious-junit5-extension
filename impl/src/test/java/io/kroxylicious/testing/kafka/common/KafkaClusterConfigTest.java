/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClusterConfigTest {

    private static final int CLIENT_BASE_PORT = 9092;
    private static final int CONTROLLER_BASE_PORT = 10092;
    private static final int INTER_BROKER_BASE_PORT = 11092;
    private static final int ANON_BASE_PORT = 12092;

    private EndpointConfig endpointConfig;
    private KafkaClusterConfig.KafkaClusterConfigBuilder kafkaClusterConfigBuilder;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        endpointConfig = new EndpointConfig();
        kafkaClusterConfigBuilder = KafkaClusterConfig.builder();
        kafkaClusterConfigBuilder.testInfo(testInfo);
    }

    @Test
    void generateConfigForSpecificBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        var kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 0);

        // Then
        assertThat(config.getNodeId()).isZero();
        assertThat(config.getAnonPort()).isEqualTo(ANON_BASE_PORT);
        assertThat(config.getExternalPort()).isEqualTo(CLIENT_BASE_PORT);
        assertThat(config.getEndpoint()).isEqualTo("localhost:" + CLIENT_BASE_PORT);
        assertThat(config.getProperties()).containsEntry("node.id", "0");
    }

    @Test
    void shouldConfigureMultipleControllersInCombinedMode() {
        // Given
        var numBrokers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(3)
                .brokersNum(numBrokers)
                .build();

        // When
        for (int nodeId = 0; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker,controller");
        }
    }

    @Test
    void shouldConfigureMultipleControllersInCombinedModeWhenExcessControllers() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numControllers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "controller");
        }
    }

    @Test
    void shouldConfigureSingleControllerInCombinedMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker");
        }
    }

    @Test
    void shouldGenerateConfigForBrokerNodesInCombinedMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getNodeConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(3);
    }

    @Test
    void shouldGenerateConfigForControllerNodesInCombinedMode() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getNodeConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(3);
    }

    ////

    @Test
    void shouldConfigureMultipleControllersInSeparateMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_SEPARATE)
                .kraftControllers(3)
                .brokersNum(numBrokers)
                .build();

        // When
        for (int nodeId = 0; nodeId < numControllers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "controller");
        }
        for (int nodeId = numControllers; nodeId < numControllers + numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker");
        }
    }

    @Test
    void shouldConfigureMultipleControllersInSeparateModeWhenExcessControllers() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_SEPARATE)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "controller");
        assertNodeIdHasRole(kafkaClusterConfig, 1, "controller");
        assertNodeIdHasRole(kafkaClusterConfig, 2, "controller");

        assertNodeIdHasRole(kafkaClusterConfig, 3, "broker");
    }

    @Test
    void shouldConfigureSingleControllerInSeparateMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_SEPARATE)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "controller");

        for (int nodeId = 1; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker");
        }
    }

    @Test
    void shouldGenerateConfigForBrokerNodesInSeparateMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_SEPARATE)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getNodeConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(4);
    }

    @Test
    void shouldGenerateConfigForControllerNodesInSeparateMode() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder
                .metadataMode(MetadataMode.KRAFT_SEPARATE)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getNodeConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(4);
    }

    private void assertNodeIdHasRole(KafkaClusterConfig kafkaClusterConfig, int nodeId, String expectedRole) {
        final var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, nodeId);
        assertThat(config.getProperties()).extracting(brokerConfig -> brokerConfig.get("process.roles")).as("nodeId: %s to have process.roles", nodeId).isEqualTo(
                expectedRole);
    }

    static class EndpointConfig implements KafkaClusterConfig.KafkaEndpoints {

        @NotNull
        private static EndpointPair generateEndpoint(int nodeId, int basePort) {
            final int port = basePort + nodeId;
            return new EndpointPair(new Endpoint("0.0.0.0", port), new Endpoint("localhost", port));
        }

        @Override
        public EndpointPair getEndpointPair(Listener listener, int nodeId) {
            switch (listener) {

                case EXTERNAL -> {
                    return generateEndpoint(nodeId, CLIENT_BASE_PORT);
                }
                case ANON -> {
                    return generateEndpoint(nodeId, ANON_BASE_PORT);
                }
                case INTERNAL -> {
                    return generateEndpoint(nodeId, INTER_BROKER_BASE_PORT);
                }
                case CONTROLLER -> {
                    return generateEndpoint(nodeId, CONTROLLER_BASE_PORT);
                }

                default -> throw new IllegalStateException("Unexpected value: " + listener);
            }
        }
    }
}
