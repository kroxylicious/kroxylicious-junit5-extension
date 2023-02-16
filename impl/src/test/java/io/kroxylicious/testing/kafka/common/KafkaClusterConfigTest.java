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
    void shouldBuildClientBootstrapAddressForSingleBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String clientBootstrapServers = kafkaClusterConfig.buildClientBootstrapServers(endpointConfig);

        // Then
        assertThat(clientBootstrapServers).doesNotContain(",");
        assertThat(clientBootstrapServers).doesNotContain("//");
        assertThat(clientBootstrapServers).contains("localhost:9092");
    }

    @Test
    void shouldBuildClientBootstrapAddressForCluster() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(3);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String clientBootstrapServers = kafkaClusterConfig.buildClientBootstrapServers(endpointConfig);

        // Then
        assertThat(clientBootstrapServers).contains(",");
        assertThat(clientBootstrapServers).doesNotContain("//");
        assertThat(clientBootstrapServers).contains("localhost:9092");
        assertThat(clientBootstrapServers).contains("localhost:9093");
        assertThat(clientBootstrapServers).contains("localhost:9094");
    }

    @Test
    void shouldBuildInterBrokerBootstrapAddressForSingleBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String interBrokerBootstrapServers = kafkaClusterConfig.buildInterBrokerBootstrapServers(endpointConfig);

        // Then
        assertThat(interBrokerBootstrapServers).doesNotContain(",");
        assertThat(interBrokerBootstrapServers).doesNotContain("//");
        assertThat(interBrokerBootstrapServers).contains("localhost:11092");
    }

    @Test
    void shouldBuildInterBrokerBootstrapAddressForCluster() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(3);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String interBrokerBootstrapServers = kafkaClusterConfig.buildInterBrokerBootstrapServers(endpointConfig);

        // Then
        assertThat(interBrokerBootstrapServers).contains(",");
        assertThat(interBrokerBootstrapServers).doesNotContain("//");
        assertThat(interBrokerBootstrapServers).contains("localhost:11092");
        assertThat(interBrokerBootstrapServers).contains("localhost:11093");
        assertThat(interBrokerBootstrapServers).contains("localhost:11094");
    }

    @Test
    void shouldBuildControllerBootstrapAddressForSingleBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String controllerBootstrapServers = kafkaClusterConfig.buildControllerBootstrapServers(endpointConfig);

        // Then
        assertThat(controllerBootstrapServers).doesNotContain(",");
        assertThat(controllerBootstrapServers).doesNotContain("//");
        assertThat(controllerBootstrapServers).contains("localhost:10092");
    }

    @Test
    void shouldBuildControllerBootstrapAddressForCluster() {
        // Given

        kafkaClusterConfigBuilder.brokersNum(3);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String controllerBootstrapServers = kafkaClusterConfig.buildControllerBootstrapServers(endpointConfig);

        // Then
        assertThat(controllerBootstrapServers).contains(",");
        assertThat(controllerBootstrapServers).doesNotContain("//");
        assertThat(controllerBootstrapServers).contains("localhost:10092");
        assertThat(controllerBootstrapServers).contains("localhost:10093");
        assertThat(controllerBootstrapServers).contains("localhost:10094");
    }

    @Test
    void shouldBuildAnonBootstrapAddressForSingleBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String anonBootstrapServers = kafkaClusterConfig.buildAnonBootstrapServers(endpointConfig);

        // Then
        assertThat(anonBootstrapServers).doesNotContain(",");
        assertThat(anonBootstrapServers).doesNotContain("//");
        assertThat(anonBootstrapServers).contains("localhost:12092");
    }

    @Test
    void shouldBuildAnonBootstrapAddressForCluster() {
        // Given

        kafkaClusterConfigBuilder.brokersNum(3);
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        final String anonBootstrapServers = kafkaClusterConfig.buildAnonBootstrapServers(endpointConfig);

        // Then
        assertThat(anonBootstrapServers).contains(",");
        assertThat(anonBootstrapServers).doesNotContain("//");
        assertThat(anonBootstrapServers).contains("localhost:12092");
        assertThat(anonBootstrapServers).contains("localhost:12093");
        assertThat(anonBootstrapServers).contains("localhost:12094");
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
