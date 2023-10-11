/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.kroxylicious.testing.kafka.api.TerminationStyle;

import static io.kroxylicious.testing.kafka.common.TestKafkaEndpoints.ANON_BASE_PORT;
import static io.kroxylicious.testing.kafka.common.TestKafkaEndpoints.CLIENT_BASE_PORT;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopologyTest {

    private TestClusterDriver driver;
    private KafkaClusterConfig.KafkaClusterConfigBuilder kafkaClusterConfigBuilder;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        driver = new TestClusterDriver();
        kafkaClusterConfigBuilder = KafkaClusterConfig.builder();
        kafkaClusterConfigBuilder.testInfo(testInfo);
    }

    @Test
    void generateConfigForSpecificBroker() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1);
        var kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        KafkaTopology kafkaTopology = KafkaTopology.create(driver, driver, kafkaClusterConfig);
        // Then
        KafkaNodeConfiguration config = kafkaTopology.get(0).configuration();
        assertThat(config.nodeId()).isZero();
        assertThat(config.anonPort()).isEqualTo(ANON_BASE_PORT);
        assertThat(config.externalPort()).isEqualTo(CLIENT_BASE_PORT);
        assertThat(config.clientEndpoint()).isEqualTo("localhost:" + CLIENT_BASE_PORT);
        assertThat(config.getProperties()).containsEntry("node.id", "0");
    }

    @Test
    void shouldConfigureMultipleControllersInCombinedMode() {
        // Given
        var numBrokers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(3).brokersNum(numBrokers).build();

        // When
        KafkaTopology kafkaTopology = KafkaTopology.create(driver, driver, kafkaClusterConfig);

        // Then
        for (int nodeId = 0; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaTopology, nodeId, "broker,controller");
        }
    }

    @Test
    void shouldConfigureMultipleControllersInControllerOnlyMode() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        KafkaTopology kafkaTopology = KafkaTopology.create(driver, driver, kafkaClusterConfig);

        // then
        assertNodeIdHasRole(kafkaTopology, 0, "broker,controller");
        for (int nodeId = 1; nodeId < numControllers; nodeId++) {
            assertNodeIdHasRole(kafkaTopology, nodeId, "controller");
        }
    }

    @Test
    void shouldConfigureSingleControllersInCombinedMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        KafkaTopology kafkaTopology = KafkaTopology.create(driver, driver, kafkaClusterConfig);

        // then
        assertNodeIdHasRole(kafkaTopology, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaTopology, nodeId, "broker");
        }
    }

    private void assertNodeIdHasRole(KafkaTopology kafkaTopology, int nodeId, String expectedRole) {
        assertThat(kafkaTopology.get(nodeId).configuration().getProperties())
                .extracting(brokerConfig -> brokerConfig.get("process.roles"))
                .as("nodeId: %s to have process.roles", nodeId).isEqualTo(expectedRole);
    }

    private static class TestClusterDriver implements KafkaClusterDriver, KafkaEndpoints {
        TestKafkaEndpoints config = new TestKafkaEndpoints();

        @Override
        public KafkaNode createNode(KafkaNodeConfiguration node) {
            return new KafkaNode() {
                @Override
                public int stop(TerminationStyle terminationStyle) {
                    return 0;
                }

                @Override
                public void start() {

                }

                @Override
                public boolean isStopped() {
                    return false;
                }

                @Override
                public int nodeId() {
                    return 0;
                }

                @Override
                public boolean isBroker() {
                    return false;
                }

                @Override
                public KafkaNodeConfiguration configuration() {
                    return node;
                }
            };
        }

        @Override
        public Zookeeper createZookeeper(ZookeeperConfig zookeeperConfig) {
            return null;
        }

        @Override
        public void nodeRemoved(KafkaNode node) {

        }

        @Override
        public void close() {

        }

        @Override
        public EndpointPair getEndpointPair(Listener listener, int nodeId) {
            return config.getEndpointPair(listener, nodeId);
        }
    }
}
