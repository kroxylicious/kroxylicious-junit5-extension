/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jetbrains.annotations.NotNull;

import kafka.server.KafkaConfig;
import lombok.Getter;

import io.kroxylicious.testing.kafka.common.KafkaEndpoints.EndpointPair;

public class KafkaNodeConfiguration {
    private static final String ONE_CONFIG = Integer.toString(1);
    private final TopologyConfiguration kafkaTopology;
    private final int nodeId;
    private final Set<Role> roles;

    @Getter
    private final KafkaClusterConfig config;

    @Getter
    private final String kafkaKraftClusterId;
    private final String brokerConnectAddress;
    private final EndpointPair externalEndpoint;
    private final EndpointPair anonEndpoint;
    private final EndpointPair internalEndpoint;
    private final EndpointPair controllerEndpoint;

    public int externalPort() {
        if (!roles.contains(Role.BROKER)) {
            throw new IllegalStateException("non-broker node doesn't have an external port");
        }
        return externalEndpoint.getConnect().getPort();
    }

    public int anonPort() {
        if (!roles.contains(Role.BROKER)) {
            throw new IllegalStateException("non-broker node doesn't have an anonymous port");
        }
        return anonEndpoint.getConnect().getPort();
    }

    public String clientEndpoint() {
        if (!roles.contains(Role.BROKER)) {
            throw new IllegalStateException("non-broker node doesn't have an client accessible endpoint");
        }
        return externalEndpoint.connectAddress();
    }

    public KafkaNodeConfiguration(TopologyConfiguration kafkaTopology, int nodeId, Set<Role> roles, KafkaClusterConfig config, KafkaEndpoints endpoints) {
        this.kafkaTopology = kafkaTopology;
        this.nodeId = nodeId;
        this.roles = roles;
        this.config = config;
        this.kafkaKraftClusterId = config.getKafkaKraftClusterId();
        if (isBroker()) {
            EndpointPair endpointPair = endpoints.getEndpointPair(Listener.EXTERNAL, nodeId());
            brokerConnectAddress = endpointPair.connectAddress();
        }
        else {
            brokerConnectAddress = null;
        }
        // allocate endpoints immediately
        if (roles.contains(Role.BROKER)) {
            externalEndpoint = endpoints.getEndpointPair(Listener.EXTERNAL, nodeId);
            anonEndpoint = endpoints.getEndpointPair(Listener.ANON, nodeId);
            internalEndpoint = endpoints.getEndpointPair(Listener.INTERNAL, nodeId);
        }
        else {
            externalEndpoint = null;
            anonEndpoint = null;
            internalEndpoint = null;
        }
        if (roles.contains(Role.CONTROLLER)) {
            controllerEndpoint = endpoints.getEndpointPair(Listener.CONTROLLER, nodeId);
        }
        else {
            controllerEndpoint = null;
        }
    }

    /**
     * Get a broker configs for a specific <code>node.id</code>.
     *
     * @return broker configuration.
     */
    @NotNull
    private Properties generateConfig() {
        Properties nodeConfiguration = new Properties();
        nodeConfiguration.putAll(config.getBrokerConfigs());
        putConfig(nodeConfiguration, "broker.id", nodeIdString());

        var protocolMap = new TreeMap<String, String>();
        var listeners = new TreeMap<String, String>();
        var advertisedListeners = new TreeMap<String, String>();
        var earlyStart = new TreeSet<String>();
        if (isBroker()) {
            configureBroker(protocolMap, listeners, advertisedListeners, earlyStart, nodeConfiguration);
        }

        if (isKraft()) {
            configureKraftNode(nodeConfiguration, protocolMap, listeners, earlyStart);
        }
        else {
            configureLegacyNode(nodeConfiguration);
        }

        putConfig(nodeConfiguration, "listener.security.protocol.map",
                protocolMap.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        putConfig(nodeConfiguration, "listeners", listeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        putConfig(nodeConfiguration, "early.start.listeners", earlyStart.stream().map(Object::toString).collect(Collectors.joining(",")));

        configureSasl(nodeConfiguration);

        putConfig(nodeConfiguration, "offsets.topic.replication.factor", ONE_CONFIG);
        // 1 partition for the __consumer_offsets_ topic should be enough
        putConfig(nodeConfiguration, "offsets.topic.num.partitions", ONE_CONFIG);
        // 1 partition for the __transaction_state_ topic should be enough
        putConfig(nodeConfiguration, "transaction.state.log.replication.factor", ONE_CONFIG);
        putConfig(nodeConfiguration, "transaction.state.log.min.isr", ONE_CONFIG);
        // Disable delay during every re-balance
        putConfig(nodeConfiguration, "group.initial.rebalance.delay.ms", Integer.toString(0));

        // The test harness doesn't rely upon Kafka JMX metrics (and probably won't because JMX isn't supported by
        // the kafka native). Registering/Unregistering the mbeans is time-consuming so we disable it.
        putConfig(nodeConfiguration, "metrics.jmx.exclude", ".*");

        return nodeConfiguration;
    }

    @NotNull
    private void configureBroker(TreeMap<String, String> protocolMap, TreeMap<String, String> listeners,
                                 TreeMap<String, String> advertisedListeners, TreeSet<String> earlyStart, Properties nodeConfiguration) {

        // - EXTERNAL: used for communications to/from consumers/producers optionally with authentication
        // - ANON: used for communications to/from consumers/producers without authentication primarily for the extension to validate the cluster
        // - INTERNAL: used for inter-broker communications (always no auth)
        // - CONTROLLER: used for inter-broker controller communications (kraft - always no auth)
        String securityProtocol = config.getSecurityProtocol();
        var externalListenerTransport = securityProtocol == null ? SecurityProtocol.PLAINTEXT.name() : securityProtocol;
        configureExternalListener(protocolMap, externalListenerTransport, listeners, externalEndpoint, advertisedListeners);
        configureInternalListener(protocolMap, listeners, internalEndpoint, advertisedListeners, earlyStart, nodeConfiguration);
        configureAnonListener(protocolMap, listeners, this.anonEndpoint, advertisedListeners);
        configureTls(externalEndpoint, nodeConfiguration, securityProtocol, config);
        putConfig(nodeConfiguration, "advertised.listeners",
                advertisedListeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
    }

    private static void configureTls(EndpointPair clientEndpoint, Properties server, String securityProtocol,
                                     KafkaClusterConfig config) {
        if (securityProtocol != null && securityProtocol.contains("SSL")) {
            KeytoolCertificateGenerator brokerKeytoolCertificateGenerator = config.getBrokerKeytoolCertificateGenerator();
            if (brokerKeytoolCertificateGenerator == null) {
                throw new RuntimeException("brokerKeytoolCertificateGenerator needs to be initialized when calling KafkaClusterConfig");
            }
            try {
                brokerKeytoolCertificateGenerator.generateSelfSignedCertificateEntry("test@kroxylicious.io", clientEndpoint.getConnect().getHost(), "Dev",
                        "Kroxylicious.io", null,
                        null,
                        "US");
                KeytoolCertificateGenerator clientKeytoolCertificateGenerator = config.getClientKeytoolCertificateGenerator();
                if (clientKeytoolCertificateGenerator != null && Path.of(clientKeytoolCertificateGenerator.getCertFilePath()).toFile().exists()) {
                    if (securityProtocol.equals(SecurityProtocol.SASL_SSL.toString())) {
                        server.put("listener.name.EXTERNAL." + BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
                    }
                    else {
                        server.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
                    }
                    brokerKeytoolCertificateGenerator.generateTrustStore(clientKeytoolCertificateGenerator.getCertFilePath(), clientEndpoint.getConnect().getHost());
                    server.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerKeytoolCertificateGenerator.getTrustStoreLocation());
                    server.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerKeytoolCertificateGenerator.getPassword());
                    server.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, brokerKeytoolCertificateGenerator.getTrustStoreType());
                }
            }
            catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException(e);
            }

            server.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, brokerKeytoolCertificateGenerator.getKeyStoreLocation());
            server.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, brokerKeytoolCertificateGenerator.getPassword());
            server.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, brokerKeytoolCertificateGenerator.getPassword());
            server.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, brokerKeytoolCertificateGenerator.getKeyStoreType());

        }
    }

    private void configureSasl(Properties server) {
        if (config.getSaslMechanism() != null) {
            putConfig(server, "sasl.enabled.mechanisms", config.getSaslMechanism());

            var saslPairs = new StringBuilder();

            Optional.ofNullable(config.getUsers()).orElse(Map.of()).forEach((key, value) -> {
                saslPairs.append(String.format("user_%s", key));
                saslPairs.append("=");
                saslPairs.append(value);
                saslPairs.append(" ");
            });

            // TODO support other than PLAIN
            String plainModuleConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required %s;", saslPairs);
            putConfig(server, String.format("listener.name.%s.plain.sasl.jaas.config", KafkaClusterConfig.EXTERNAL_LISTENER_NAME.toLowerCase()), plainModuleConfig);
        }
    }

    private static void configureInternalListener(TreeMap<String, String> protocolMap, TreeMap<String, String> listeners,
                                                  EndpointPair interBrokerEndpoint,
                                                  TreeMap<String, String> advertisedListeners, TreeSet<String> earlyStart, Properties server) {
        protocolMap.put(KafkaClusterConfig.INTERNAL_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());
        listeners.put(KafkaClusterConfig.INTERNAL_LISTENER_NAME, interBrokerEndpoint.listenAddress());
        advertisedListeners.put(KafkaClusterConfig.INTERNAL_LISTENER_NAME, interBrokerEndpoint.advertisedAddress());
        earlyStart.add(KafkaClusterConfig.INTERNAL_LISTENER_NAME);
        putConfig(server, "inter.broker.listener.name", KafkaClusterConfig.INTERNAL_LISTENER_NAME);
    }

    private static void configureAnonListener(TreeMap<String, String> protocolMap, TreeMap<String, String> listeners,
                                              EndpointPair anonEndpoint,
                                              TreeMap<String, String> advertisedListeners) {
        protocolMap.put(KafkaClusterConfig.ANON_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());
        listeners.put(KafkaClusterConfig.ANON_LISTENER_NAME, anonEndpoint.listenAddress());
        advertisedListeners.put(KafkaClusterConfig.ANON_LISTENER_NAME, anonEndpoint.advertisedAddress());
    }

    private static void configureExternalListener(TreeMap<String, String> protocolMap, String externalListenerTransport, TreeMap<String, String> listeners,
                                                  EndpointPair clientEndpoint, TreeMap<String, String> advertisedListeners) {
        protocolMap.put(KafkaClusterConfig.EXTERNAL_LISTENER_NAME, externalListenerTransport);
        listeners.put(KafkaClusterConfig.EXTERNAL_LISTENER_NAME, clientEndpoint.listenAddress());
        advertisedListeners.put(KafkaClusterConfig.EXTERNAL_LISTENER_NAME, clientEndpoint.advertisedAddress());
    }

    private void configureLegacyNode(Properties server) {
        Optional<ZookeeperConfig> zookeeperConfig = kafkaTopology.getZookeeperConfig();
        if (zookeeperConfig.isEmpty()) {
            throw new IllegalStateException("topology had no zookeeper config but we are in non-kraft mode");
        }
        putConfig(server, "zookeeper.connect", zookeeperConfig.get().connectAddress());
        putConfig(server, "zookeeper.sasl.enabled", "false");
        putConfig(server, "zookeeper.connection.timeout.ms", Long.toString(60000));
        putConfig(server, KafkaConfig.ZkSessionTimeoutMsProp(), Long.toString(6000));
    }

    private void configureKraftNode(Properties nodeConfiguration, TreeMap<String, String> protocolMap,
                                    TreeMap<String, String> listeners,
                                    TreeSet<String> earlyStart) {
        putConfig(nodeConfiguration, "node.id", nodeIdString()); // Required by Kafka 3.3 onwards.

        var quorumVoters = kafkaTopology.getQuorumVoters();
        putConfig(nodeConfiguration, "controller.quorum.voters", quorumVoters);
        putConfig(nodeConfiguration, "controller.listener.names", KafkaClusterConfig.CONTROLLER_LISTENER_NAME);
        protocolMap.put(KafkaClusterConfig.CONTROLLER_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());

        putConfig(nodeConfiguration, "process.roles", rolesConfigString());
        if (isController()) {
            final String bindAddress = controllerEndpoint.getBind().toString();
            listeners.put(KafkaClusterConfig.CONTROLLER_LISTENER_NAME, bindAddress);
            earlyStart.add(KafkaClusterConfig.CONTROLLER_LISTENER_NAME);
        }
    }

    private static void putConfig(Properties server, String key, String value) {
        var orig = server.put(key, value);
        if (orig != null) {
            throw new RuntimeException("Cannot override broker config '" + key + "=" + value + "' with new value " + orig);
        }
    }

    public int nodeId() {
        return nodeId;
    }

    public boolean isBroker() {
        return roles.contains(Role.BROKER);
    }

    public boolean isController() {
        return roles.contains(Role.CONTROLLER);
    }

    public Optional<String> getBrokerConnectAddress() {
        return Optional.of(brokerConnectAddress);
    }

    public boolean isKraft() {
        return kafkaTopology.isKraftMode();
    }

    public String rolesConfigString() {
        return roles.stream().map(Role::getConfigString).sorted().collect(Collectors.joining(","));
    }

    public String nodeIdString() {
        return Integer.toString(nodeId);
    }

    public Properties getProperties() {
        return generateConfig();
    }

    public Optional<String> controllerQuorumAddress() {
        if (!roles.contains(Role.CONTROLLER)) {
            return Optional.empty();
        }
        String quorumAddress = String.format("%d@//%s", nodeId, controllerEndpoint.connectAddress());
        return Optional.of(quorumAddress);
    }

    public String getAnonymousConnectAddress() {
        return anonEndpoint.connectAddress();
    }

    @Override
    public String toString() {
        return "KafkaNodeConfiguration{" +
                "nodeId=" + nodeId +
                ", roles=" + roles +
                ", kraftMode=" + isKraft() +
                '}';
    }
}
