/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(config.nodeId()).isZero();
        assertThat(config.properties())
                .containsEntry("node.id", "0")
                .containsEntry("controller.quorum.voters", "0@localhost:" + CONTROLLER_BASE_PORT)
                .containsEntry("listeners", "ANON://0.0.0.0:%d,CONTROLLER://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d,INTERNAL://0.0.0.0:%d".formatted(ANON_BASE_PORT,
                        CONTROLLER_BASE_PORT, CLIENT_BASE_PORT, INTER_BROKER_BASE_PORT));
    }

    @Test
    void shouldConfigureMultipleControllersInCombinedMode() {
        // Given
        var numBrokers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(3).brokersNum(numBrokers).build();

        // When
        for (int nodeId = 0; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker,controller");
        }
    }

    @Test
    void shouldConfigureMultipleControllersInControllerOnlyMode() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numControllers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "controller");
        }
    }

    @Test
    void shouldConfigureSingleControllersInCombinedMode() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        assertNodeIdHasRole(kafkaClusterConfig, 0, "broker,controller");

        for (int nodeId = 1; nodeId < numBrokers; nodeId++) {
            assertNodeIdHasRole(kafkaClusterConfig, nodeId, "broker");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "3.9.0", "latest" })
    void shouldAdvertiseControllerListenerInCombinedMode(String version) {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1).kafkaVersion(version);
        var kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 0);

        // Then
        assertThat(config.properties())
                .containsEntry("process.roles", "broker,controller")
                .hasEntrySatisfying("advertised.listeners", value -> {
                    assertThat(value)
                            .asInstanceOf(InstanceOfAssertFactories.STRING)
                            .contains("CONTROLLER://localhost:" + CONTROLLER_BASE_PORT);
                });
    }

    @Test
    void shouldNotAdvertiseControllerListenerForBrokerNodes() {
        // Given
        kafkaClusterConfigBuilder.brokersNum(2).kraftControllers(1);
        var kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 1);

        // Then
        assertThat(config.properties())
                .containsEntry("process.roles", "broker")
                .hasEntrySatisfying("advertised.listeners", value -> {
                    assertThat(value)
                            .asInstanceOf(InstanceOfAssertFactories.STRING)
                            .doesNotContain("CONTROLLER://");
                });
    }

    @Test
    void shouldGenerateConfigForBrokerWithZookeeperController() {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(false).brokersNum(1).build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 0);

        // Then
        assertThat(config.properties())
                .doesNotContainKey("process.roles")
                .containsEntry("zookeeper.connect", "localhost:" + CONTROLLER_BASE_PORT);

    }

    @ParameterizedTest
    @ValueSource(strings = { "3.8.0", "3.7.0" })
    void shouldNotAdvertiseControllerListenerForOlderKafkaVersions(String version) {
        // Given
        kafkaClusterConfigBuilder.brokersNum(1).kafkaVersion(version);
        var kafkaClusterConfig = kafkaClusterConfigBuilder.build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 1);

        // Then
        assertThat(config.properties())
                .containsEntry("process.roles", "broker")
                .hasEntrySatisfying("advertised.listeners", value -> {
                    assertThat(value)
                            .asInstanceOf(InstanceOfAssertFactories.STRING)
                            .doesNotContain("CONTROLLER://localhost:10092");
                });
    }

    @Test
    void shouldGenerateConfigForBrokerNodes() {
        // Given
        var numBrokers = 3;
        var numControllers = 1;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getBrokerConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(3);
    }

    @Test
    void shouldGenerateConfigForControllerNodes() {
        // Given
        var numBrokers = 1;
        var numControllers = 3;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true).kraftControllers(numControllers).brokersNum(numBrokers).build();

        // When
        final Stream<KafkaClusterConfig.ConfigHolder> brokerConfigs = kafkaClusterConfig.getBrokerConfigs(() -> endpointConfig);

        // Then
        assertThat(brokerConfigs).hasSize(3);
    }

    @ParameterizedTest
    @ValueSource(strings = { "3.7.0", "3.8.0" })
    void olderKafkaControllerShouldNotAdvertisedControllerListener(String version) {
        // Given
        var numBrokers = 1;
        var numControllers = 2;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .kafkaVersion(version)
                .build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 1);

        // Then
        assertThat(config.properties())
                .containsEntry("process.roles", "controller")
                .doesNotContainKeys("advertised.listeners");
    }

    @ParameterizedTest
    @ValueSource(strings = { "3.9.0" })
    void newerKafkaControllerShouldAdvertisedControllerListener(String version) {
        // Given
        var numBrokers = 1;
        var numControllers = 2;
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.kraftMode(true)
                .kraftControllers(numControllers)
                .brokersNum(numBrokers)
                .kafkaVersion(version)
                .build();

        // When
        var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, 1);

        // Then
        assertThat(config.properties())
                .containsEntry("process.roles", "controller")
                .hasEntrySatisfying("advertised.listeners", value -> {
                    assertThat(value)
                            .asInstanceOf(InstanceOfAssertFactories.STRING)
                            .contains("CONTROLLER://localhost:" + (CONTROLLER_BASE_PORT + 1));
                });
    }

    @Test
    void shouldRejectBadInvalidMechanism() {
        // Given
        final KafkaClusterConfig kafkaClusterConfig = kafkaClusterConfigBuilder.saslMechanism("FOO").build();

        // When/Then
        assertThatThrownBy(() -> kafkaClusterConfig.getBrokerConfigs(() -> endpointConfig))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void kafkaVersionDisallowsNulls() {
        assertThatThrownBy(() -> kafkaClusterConfigBuilder.kafkaVersion(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void noConstraints() {
        // Given
        var annotations = getAnnotations();

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSaslMechanism()).isNull();
        assertThat(config.getSecurityProtocol()).isEqualTo("PLAINTEXT");
    }

    @Test
    void saslMechanismValueDefaultsToPlain() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslMechanism(null, Map.of("alice", "secret")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSaslMechanism()).isEqualTo("PLAIN");
        assertThat(config.getUsers())
                .hasSize(1)
                .containsEntry("alice", "secret");
    }

    @Test
    void constraintUserWithSaslMechanism() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslMechanism("SCRAM-SHA-256", Map.of("alice", "secret")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSaslMechanism()).isEqualTo("SCRAM-SHA-256");
        assertThat(config.getUsers())
                .hasSize(1)
                .containsEntry("alice", "secret");
    }

    @Test
    void constraintManyUsers() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslMechanism("SCRAM-SHA-256", Map.of("alice", "secret", "bob", "secret")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getUsers())
                .containsOnlyKeys("alice", "bob");
    }

    @Test
    void constraintSaslPlainAuthDeprecatedAnnotation() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslPlainAuth("bob", "secret"));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSaslMechanism()).isEqualTo("PLAIN");
        assertThat(config.getUsers())
                .hasSize(1)
                .containsEntry("bob", "secret");
    }

    @Test
    void constraintSaslPlainAuthManyDeprecatedAnnotation() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslPlainAuth(Map.of("alice", "secret", "bob", "secret")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSaslMechanism()).isEqualTo("PLAIN");
        assertThat(config.getUsers())
                .hasSize(2)
                .containsKeys("bob", "alice");
    }

    @Test
    void constraintSaslMechanismAndSaslPlainAuthCombinationDisallowed() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.saslMechanism("PLAIN"), ConstraintUtils.saslPlainAuth("bob", "secret"));

        // When/Then
        assertThatThrownBy(() -> KafkaClusterConfig.fromConstraints(annotations, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot use deprecated SaslPlainAuth with SaslMechanism");

    }

    @Test
    void constraintBrokerConfig() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.brokerConfig("compression.type", "zstd"));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getBrokerConfigs())
                .hasSize(1)
                .containsEntry("compression.type", "zstd");
    }

    @Test
    void constraintManyBrokerConfig() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.brokerConfigs(Map.of("compression.type", "zstd", "delete.topic.enable", "true")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getBrokerConfigs())
                .containsOnlyKeys("compression.type", "delete.topic.enable");
    }

    @Test
    void constraintKraft() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.kraftCluster(2));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.isKraftMode())
                .isTrue();
        assertThat(config.getKraftControllers())
                .isEqualTo(2);
    }

    @Test
    void constraintZookeeper() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.zooKeeperCluster());

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.isKraftMode())
                .isFalse();
    }

    @Test
    void constraintBroker() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.brokerCluster(2));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getBrokersNum())
                .isEqualTo(2);
    }

    @Test
    void constraintVersion() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.version("0.1"));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getKafkaVersion())
                .isEqualTo("0.1");
    }

    @Test
    void constraintClusterId() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.clusterId("1234"));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getKafkaKraftClusterId())
                .isEqualTo("1234");
    }

    @Test
    void constraintTls() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.tls());

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSecurityProtocol())
                .isEqualTo("SSL");
    }

    @Test
    void constraintTlsAndSasl() {
        // Given
        var annotations = getAnnotations(ConstraintUtils.tls(), ConstraintUtils.saslMechanism("PLAIN", Map.of("alice", "secret")));

        // When
        var config = KafkaClusterConfig.fromConstraints(annotations, null);

        // Then
        assertThat(config.getSecurityProtocol())
                .isEqualTo("SASL_SSL");
    }

    private List<Annotation> getAnnotations(Annotation... annotations) {
        return List.of(annotations);
    }

    private void assertNodeIdHasRole(KafkaClusterConfig kafkaClusterConfig, int nodeId, String expectedRole) {
        final var config = kafkaClusterConfig.generateConfigForSpecificNode(endpointConfig, nodeId);
        assertThat(config.properties()).extracting(brokerConfig -> brokerConfig.get("process.roles")).as("nodeId: %s to have process.roles", nodeId).isEqualTo(
                expectedRole);
    }

    static class EndpointConfig implements KafkaListenerSource {

        @NonNull
        private static KafkaListener generateEndpoint(int nodeId, int basePort) {
            final int port = basePort + nodeId;
            return new KafkaListener(new KafkaEndpoint("0.0.0.0", port), new KafkaEndpoint("localhost", port), new KafkaEndpoint("localhost", port));
        }

        @Override
        public KafkaListener getKafkaListener(Listener listener, int nodeId) {
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

    static Stream<Arguments> supportedConstraints() {
        return Stream.of(Arguments.of(Deprecated.class, false),
                Arguments.of(BrokerCluster.class, true));
    }

    @ParameterizedTest
    @MethodSource
    void supportedConstraints(Class<Annotation> anno, boolean supported) {
        assertThat(KafkaClusterConfig.supportsConstraint(anno)).isEqualTo(supported);
    }
}
