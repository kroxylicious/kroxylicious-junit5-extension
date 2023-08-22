/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

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
        assertThat(config.getBrokerNum()).isZero();
        assertThat(config.getAnonPort()).isEqualTo(ANON_BASE_PORT);
        assertThat(config.getExternalPort()).isEqualTo(CLIENT_BASE_PORT);
        assertThat(config.getEndpoint()).isEqualTo("localhost:" + CLIENT_BASE_PORT);
        assertThat(config.getProperties()).containsEntry("node.id", "0");
    }

    @Test
    void shouldConfigureMultipleControllersInCombinedMode() {
        //Given
        var numBrokers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(3).brokersNum(numBrokers).build();

        //When
        for (int nodeId = 0; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker,controller");
        }
    }

    @Test
    void shouldConfigureMultipleControllersInControllerOnlyMode() {
        //Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        //When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numControllers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "controller");
        }
    }

    @Test
    void shouldConfigureSingleControllersInCombinedMode() {
        //Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        //When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker");
        }
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
