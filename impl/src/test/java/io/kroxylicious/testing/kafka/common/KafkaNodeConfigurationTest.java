/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import kafka.server.KafkaConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class KafkaNodeConfigurationTest {

    public static final String QUORUM_VOTERS = "0@localhost:1234";
    private static final TestKafkaEndpoints ENDPOINTS = new TestKafkaEndpoints();

    @ParameterizedTest
    @MethodSource(value = "allVariants")
    public void testBrokerIdSet(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("broker.id")).isEqualTo(configuration.nodeIdString());
    }

    @Test
    public void testZookeeperConfiguration() {
        // given
        KafkaNodeConfiguration configuration = legacyBroker();

        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("zookeeper.connect")).isEqualTo("localhost:" + TestKafkaEndpoints.CONTROLLER_BASE_PORT);
        assertThat(properties.get("zookeeper.sasl.enabled")).isEqualTo("false");
        assertThat(properties.get("zookeeper.connection.timeout.ms")).isEqualTo("60000");
        assertThat(properties.get(KafkaConfig.ZkSessionTimeoutMsProp())).isEqualTo("6000");
    }

    @ParameterizedTest
    @MethodSource(value = "kraftVariants")
    public void testCommonKraftConfiguration(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();
        assertThat(properties.get(KafkaConfig.NodeIdProp())).isEqualTo(configuration.nodeIdString());
        assertThat(properties.get(KafkaConfig.ProcessRolesProp())).isEqualTo(configuration.rolesConfigString());
        assertThat(properties.get(KafkaConfig.QuorumVotersProp())).isEqualTo(QUORUM_VOTERS);
        assertThat(properties.get(KafkaConfig.ControllerListenerNamesProp())).isEqualTo("CONTROLLER");
        assertThat(properties.get(KafkaConfig.ListenerSecurityProtocolMapProp())).asString().contains("CONTROLLER:PLAINTEXT");
    }

    @ParameterizedTest
    @MethodSource(value = "kraftControllerVariants")
    public void testKraftControllerListenerConfiguration(KafkaNodeConfiguration configuration) {

        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get(KafkaConfig.ListenersProp())).asString().contains("CONTROLLER");
        assertThat(properties.get(KafkaConfig.EarlyStartListenersProp())).asString().contains("CONTROLLER");
    }

    @Test
    public void testKraftBrokerHasNoControllerListener() {
        // given
        KafkaNodeConfiguration configuration = kraftBroker();

        // when
        Properties properties = configuration.getProperties();
        assertThat(properties.get(KafkaConfig.ListenersProp())).asString().doesNotContain("CONTROLLER");
        assertThat(properties.get(KafkaConfig.EarlyStartListenersProp())).asString().doesNotContain("CONTROLLER");
    }

    @Test
    public void testZookeeperDoesNotSetKraftConfiguration() {
        // given
        KafkaNodeConfiguration configuration = legacyBroker();

        // when
        Properties properties = configuration.getProperties();
        assertThat(properties.get(KafkaConfig.NodeIdProp())).isNull();
        assertThat(properties.get(KafkaConfig.ProcessRolesProp())).isNull();
        assertThat(properties.get(KafkaConfig.QuorumVotersProp())).isNull();
        assertThat(properties.get(KafkaConfig.ControllerListenerNamesProp())).isNull();
        assertThat(properties.get(KafkaConfig.ListenersProp())).asString().doesNotContain("CONTROLLER");
        assertThat(properties.get(KafkaConfig.EarlyStartListenersProp())).asString().doesNotContain("CONTROLLER");
        assertThat(properties.get(KafkaConfig.ListenerSecurityProtocolMapProp())).asString().doesNotContain("CONTROLLER:PLAINTEXT");
    }

    @ParameterizedTest
    @MethodSource(value = "kraftVariants")
    public void testZookeeperConfigurationNotPresentInKraftMode(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get(KafkaConfig.ZkConnectProp())).isNull();
        assertThat(properties.get(KafkaConfig.ZkSslEnabledProtocolsDoc())).isNull();
        assertThat(properties.get("zookeeper.connection.timeout.ms")).isNull();
        assertThat(properties.get(KafkaConfig.ZkSessionTimeoutMsProp())).isNull();
    }

    @ParameterizedTest
    @MethodSource(value = "allVariants")
    public void testMetricsDisabled(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("metrics.jmx.exclude")).isEqualTo(".*");
    }

    @ParameterizedTest
    @MethodSource(value = "allVariants")
    public void testGroupRebalanceDelayIsAlwaysZero(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("group.initial.rebalance.delay.ms")).isEqualTo("0");
    }

    @ParameterizedTest
    @MethodSource(value = "allVariants")
    public void testOffsetTopicConfiguration(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("offsets.topic.replication.factor")).isEqualTo("1");
        assertThat(properties.get("offsets.topic.num.partitions")).isEqualTo("1");
    }

    @ParameterizedTest
    @MethodSource(value = "allVariants")
    public void testTransactionStateTopicConfiguration(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("transaction.state.log.replication.factor")).isEqualTo("1");
        assertThat(properties.get("transaction.state.log.min.isr")).isEqualTo("1");
    }

    @ParameterizedTest
    @MethodSource(value = "allBrokerVariants")
    public void testBrokerAdvertisedListenersConfiguration(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("advertised.listeners")).asString()
                .contains("ANON://" + ENDPOINTS.getEndpointPair(Listener.ANON, configuration.nodeId()).connectAddress())
                .contains("EXTERNAL://" + ENDPOINTS.getEndpointPair(Listener.EXTERNAL, configuration.nodeId()).connectAddress())
                .contains("INTERNAL://" + ENDPOINTS.getEndpointPair(Listener.INTERNAL, configuration.nodeId()).connectAddress());
    }

    @ParameterizedTest
    @MethodSource(value = "allBrokerVariants")
    public void testBrokerListenersConfiguration(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get("listeners")).asString()
                .contains("ANON:" + ENDPOINTS.getEndpointPair(Listener.ANON, configuration.nodeId()).listenAddress())
                .contains("EXTERNAL:" + ENDPOINTS.getEndpointPair(Listener.EXTERNAL, configuration.nodeId()).listenAddress())
                .contains("INTERNAL:" + ENDPOINTS.getEndpointPair(Listener.INTERNAL, configuration.nodeId()).listenAddress());
    }

    @ParameterizedTest
    @MethodSource(value = "allBrokerVariants")
    public void testBrokerSecurityProtocolMap(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get(KafkaConfig.ListenerSecurityProtocolMapProp())).asString()
                .contains("ANON:PLAINTEXT")
                .contains("EXTERNAL:PLAINTEXT")
                .contains("INTERNAL:PLAINTEXT");
    }

    @ParameterizedTest
    @MethodSource(value = "allBrokerVariants")
    public void testAllBrokersSetInterBrokerListenerNameToInternal(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get(KafkaConfig.InterBrokerListenerNameProp())).asString()
                .isEqualTo("INTERNAL");
    }

    @ParameterizedTest
    @MethodSource(value = "allBrokerVariants")
    public void testAllBrokersSetInternalListenerToEarlyStart(KafkaNodeConfiguration configuration) {
        // when
        Properties properties = configuration.getProperties();

        // then
        assertThat(properties.get(KafkaConfig.EarlyStartListenersProp())).asString()
                .contains("INTERNAL");
    }

    static Stream<KafkaNodeConfiguration> allVariants() {
        return Stream.concat(Stream.of(legacyBroker()), kraftVariants());
    }

    static Stream<KafkaNodeConfiguration> allBrokerVariants() {
        return Stream.of(kraftBroker(), kraftCombinedMode(), legacyBroker());
    }

    static Stream<KafkaNodeConfiguration> kraftVariants() {
        return Stream.concat(Stream.of(kraftBroker()), kraftControllerVariants());
    }

    static Stream<KafkaNodeConfiguration> kraftControllerVariants() {
        return Stream.of(kraftController(), kraftCombinedMode());
    }

    @NotNull
    private static KafkaNodeConfiguration kraftBroker() {
        return createKraftNodeConfig(Set.of(Role.BROKER));
    }

    @NotNull
    private static KafkaNodeConfiguration kraftCombinedMode() {
        return createKraftNodeConfig(Set.of(Role.BROKER, Role.CONTROLLER));
    }

    @NotNull
    private static KafkaNodeConfiguration kraftController() {
        return createKraftNodeConfig(Set.of(Role.CONTROLLER));
    }

    @NotNull
    private static KafkaNodeConfiguration legacyBroker() {
        TopologyConfiguration mock = Mockito.mock(TopologyConfiguration.class);
        when(mock.isKraftMode()).thenReturn(false);
        when(mock.getZookeeperConfig()).thenReturn(Optional.of(new ZookeeperConfig(ENDPOINTS)));
        return new KafkaNodeConfiguration(mock, 1, Set.of(Role.BROKER), KafkaClusterConfig.builder().build(), ENDPOINTS);
    }

    @NotNull
    private static KafkaNodeConfiguration createKraftNodeConfig(Set<Role> roles) {
        TopologyConfiguration mock = kraftTopology();
        return new KafkaNodeConfiguration(mock, 1, roles, KafkaClusterConfig.builder().build(), ENDPOINTS);
    }

    @NotNull
    private static TopologyConfiguration kraftTopology() {
        TopologyConfiguration mock = Mockito.mock(TopologyConfiguration.class);
        when(mock.isKraftMode()).thenReturn(true);
        when(mock.getQuorumVoters()).thenReturn(QUORUM_VOTERS);
        return mock;
    }

}
