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
        final var config = kafkaClusterConfig.generateConfigForSpecificBroker(endpointConfig, 0);

        // Then
        assertThat(config.getBrokerNum()).isEqualTo(0);
        assertThat(config.getAnonPort()).isEqualTo(ANON_BASE_PORT);
        assertThat(config.getExternalPort()).isEqualTo(CLIENT_BASE_PORT);
        assertThat(config.getEndpoint()).isEqualTo("localhost:" + CLIENT_BASE_PORT);
        assertThat(config.getProperties()).containsEntry("node.id", "0");
    }

    static class EndpointConfig implements KafkaClusterConfig.KafkaEndpoints {

        @Override
        public EndpointPair getInterBrokerEndpoint(int brokerId) {
            return generateEndpoint(brokerId, INTER_BROKER_BASE_PORT);
        }

        @Override
        public EndpointPair getControllerEndpoint(int brokerId) {
            return generateEndpoint(brokerId, CONTROLLER_BASE_PORT);
        }

        @Override
        public EndpointPair getClientEndpoint(int brokerId) {
            return generateEndpoint(brokerId, CLIENT_BASE_PORT);
        }

        @Override
        public EndpointPair getAnonEndpoint(int brokerId) {
            return generateEndpoint(brokerId, ANON_BASE_PORT);
        }

        @NotNull
        private static EndpointPair generateEndpoint(int brokerId, int basePort) {
            final int port = basePort + brokerId;
            return new EndpointPair(new Endpoint("0.0.0.0", port), new Endpoint("localhost", port));
        }
    }
}
