/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInfo;

import kafka.server.KafkaConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig.KafkaEndpoints.Listener;

/**
 * The Kafka cluster config class.
 */
@Builder(toBuilder = true)
@Getter
@ToString
public class KafkaClusterConfig {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterConfig.class.getName());
    private static final String ONE_CONFIG = Integer.toString(1);

    private TestInfo testInfo;
    private KeytoolCertificateGenerator brokerKeytoolCertificateGenerator;
    private KeytoolCertificateGenerator clientKeytoolCertificateGenerator;

    /**
     * specifies the cluster execution mode.
     */
    private final KafkaClusterExecutionMode execMode;

    /**
     * if true, the cluster will be brought up in Kraft-mode
     */
    private final Boolean kraftMode;

    /**
     * Kafka version to be used for deploying kafka in container mode, e.g. "3.3.1".
     */
    @Builder.Default
    private String kafkaVersion = "latest";

    /**
     * name of SASL mechanism to be configured on kafka for the external listener, if null, anonymous communication
     * will be used.
     */
    private final String saslMechanism;
    private final String securityProtocol;
    @Builder.Default
    private Integer brokersNum = 1;

    @Builder.Default
    private Integer kraftControllers = 1;

    @Builder.Default
    private String kafkaKraftClusterId = Uuid.randomUuid().toString();
    /**
     * The users and passwords to be configured into the server's JAAS configuration used for the external listener.
     */
    @Singular
    private final Map<String, String> users;

    @Singular
    private final Map<String, String> brokerConfigs;

    private static final Set<Class<? extends Annotation>> SUPPORTED_CONSTRAINTS = Set.of(
            ClusterId.class,
            BrokerCluster.class,
            BrokerConfig.class,
            BrokerConfig.List.class,
            KRaftCluster.class,
            Tls.class,
            SaslPlainAuth.class,
            SaslPlainAuth.List.class,
            ZooKeeperCluster.class,
            Version.class);

    /**
     * Does the KafkaClusterConfiguration support the supplied constraint.
     *
     * @param annotation the annotation
     * @return the boolean
     */
    public static boolean supportsConstraint(Class<? extends Annotation> annotation) {
        return SUPPORTED_CONSTRAINTS.contains(annotation);
    }

    /**
     * Build the cluster constraints from the supplied list of annotations.
     *
     * @param annotations the annotations used to configure the KafkaCluster
     * @return the kafka cluster config
     */
    public static KafkaClusterConfig fromConstraints(List<Annotation> annotations) {
        var builder = builder();
        builder.brokersNum(1);
        boolean sasl = false;
        boolean tls = false;
        for (Annotation annotation : annotations) {
            if (annotation instanceof BrokerCluster brokerCluster) {
                builder.brokersNum(brokerCluster.numBrokers());
            }
            if (annotation instanceof KRaftCluster kRaftCluster) {
                builder.kraftMode(true);
                builder.kraftControllers(kRaftCluster.numControllers());
            }
            if (annotation instanceof ZooKeeperCluster) {
                builder.kraftMode(false);
            }
            if (annotation instanceof Tls) {
                tls = true;
                try {
                    builder.brokerKeytoolCertificateGenerator(new KeytoolCertificateGenerator());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (annotation instanceof SaslPlainAuth.List saslPlainAuthList) {
                builder.saslMechanism("PLAIN");
                sasl = true;
                Map<String, String> users = new HashMap<>();
                for (var user : saslPlainAuthList.value()) {
                    users.put(user.user(), user.password());
                }
                builder.users(users);
            }
            else if (annotation instanceof SaslPlainAuth saslPlainAuth) {
                builder.saslMechanism("PLAIN");
                sasl = true;
                builder.users(Map.of(saslPlainAuth.user(), saslPlainAuth.password()));
            }
            if (annotation instanceof ClusterId clusterId) {
                builder.kafkaKraftClusterId(clusterId.value());
            }
            if (annotation instanceof Version version) {
                builder.kafkaVersion(version.value());
            }
            if (annotation instanceof BrokerConfig.List brokerConfigList) {
                for (var config : brokerConfigList.value()) {
                    builder.brokerConfig(config.name(), config.value());
                }
            }
            else if (annotation instanceof BrokerConfig brokerConfig) {
                builder.brokerConfig(brokerConfig.name(), brokerConfig.value());
            }
        }
        builder.securityProtocol((sasl ? "SASL_" : "") + (tls ? "SSL" : "PLAINTEXT"));
        return builder.build();
    }

    /**
     * Gets broker configs.
     *
     * @param endPointConfigSupplier the end point config supplier
     * @return the broker configs
     */
    public Stream<ConfigHolder> getBrokerConfigs(Supplier<KafkaEndpoints> endPointConfigSupplier) {
        List<ConfigHolder> properties = new ArrayList<>();
        KafkaEndpoints kafkaEndpoints = endPointConfigSupplier.get();
        for (int brokerNum = 0; brokerNum < brokersNum; brokerNum++) {
            final ConfigHolder brokerConfigHolder = generateConfigForSpecificNode(kafkaEndpoints, brokerNum);
            properties.add(brokerConfigHolder);
        }

        return properties.stream();
    }

    /**
     * Get a broker configs for a specific <code>node.id</code>.
     *
     * @param kafkaEndpoints the end point config
     * @param nodeId kafka <code>node.id</code>
     * @return broker configuration.
     */
    @NotNull
    public ConfigHolder generateConfigForSpecificNode(KafkaEndpoints kafkaEndpoints, int nodeId) {
        Properties server = new Properties();
        server.putAll(brokerConfigs);

        putConfig(server, "broker.id", Integer.toString(nodeId));

        var interBrokerEndpoint = kafkaEndpoints.getEndpointPair(Listener.INTERNAL, nodeId);
        var clientEndpoint = kafkaEndpoints.getEndpointPair(Listener.EXTERNAL, nodeId);
        var anonEndpoint = kafkaEndpoints.getEndpointPair(Listener.ANON, nodeId);

        // - EXTERNAL: used for communications to/from consumers/producers optionally with authentication
        // - ANON: used for communications to/from consumers/producers without authentication primarily for the extension to validate the cluster
        // - INTERNAL: used for inter-broker communications (always no auth)
        // - CONTROLLER: used for inter-broker controller communications (kraft - always no auth)

        var externalListenerTransport = securityProtocol == null ? SecurityProtocol.PLAINTEXT.name() : securityProtocol;

        var protocolMap = new TreeMap<String, String>();
        var listeners = new TreeMap<String, String>();
        var advertisedListeners = new TreeMap<String, String>();
        var earlyStart = new TreeSet<String>();

        protocolMap.put("EXTERNAL", externalListenerTransport);
        listeners.put("EXTERNAL", clientEndpoint.listenAddress());
        advertisedListeners.put("EXTERNAL", clientEndpoint.advertisedAddress());

        protocolMap.put("ANON", SecurityProtocol.PLAINTEXT.name());
        listeners.put("ANON", anonEndpoint.listenAddress());
        advertisedListeners.put("ANON", anonEndpoint.advertisedAddress());

        protocolMap.put("INTERNAL", SecurityProtocol.PLAINTEXT.name());
        listeners.put("INTERNAL", interBrokerEndpoint.listenAddress());
        advertisedListeners.put("INTERNAL", interBrokerEndpoint.advertisedAddress());
        earlyStart.add("INTERNAL");
        putConfig(server, "inter.broker.listener.name", "INTERNAL");

        if (isKraftMode()) {
            putConfig(server, "node.id", Integer.toString(nodeId)); // Required by Kafka 3.3 onwards.

            var quorumVoters = IntStream.range(0, kraftControllers)
                    .mapToObj(controllerId -> String.format("%d@//%s", controllerId, kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, controllerId).connectAddress()))
                    .collect(Collectors.joining(","));
            putConfig(server, "controller.quorum.voters", quorumVoters);
            putConfig(server, "controller.listener.names", "CONTROLLER");
            protocolMap.put("CONTROLLER", SecurityProtocol.PLAINTEXT.name());

            if (nodeId == 0) {
                var controllerEndpoint = kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, nodeId);
                putConfig(server, "process.roles", "broker,controller");

                listeners.put("CONTROLLER", controllerEndpoint.getBind().toString());
                earlyStart.add("CONTROLLER");
            }
            else {
                putConfig(server, "process.roles", "broker");
            }
        }
        else {
            putConfig(server, "zookeeper.connect", kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, 0).connectAddress());
            putConfig(server, "zookeeper.sasl.enabled", "false");
            putConfig(server, "zookeeper.connection.timeout.ms", Long.toString(60000));
            putConfig(server, KafkaConfig.ZkSessionTimeoutMsProp(), Long.toString(6000));
        }

        putConfig(server, "listener.security.protocol.map",
                protocolMap.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        putConfig(server, "listeners", listeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        putConfig(server, "advertised.listeners", advertisedListeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        putConfig(server, "early.start.listeners", earlyStart.stream().map(Object::toString).collect(Collectors.joining(",")));

        if (saslMechanism != null) {
            putConfig(server, "sasl.enabled.mechanisms", saslMechanism);

            var saslPairs = new StringBuilder();

            Optional.ofNullable(users).orElse(Map.of()).forEach((key, value) -> {
                saslPairs.append(String.format("user_%s", key));
                saslPairs.append("=");
                saslPairs.append(value);
                saslPairs.append(" ");
            });

            // TODO support other than PLAIN
            String plainModuleConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required %s;", saslPairs);
            putConfig(server, String.format("listener.name.%s.plain.sasl.jaas.config", "EXTERNAL".toLowerCase()), plainModuleConfig);
        }

        if (securityProtocol != null && securityProtocol.contains("SSL")) {
            if (brokerKeytoolCertificateGenerator == null) {
                throw new RuntimeException("brokerKeytoolCertificateGenerator needs to be initialized when calling KafkaClusterConfig");
            }
            try {
                brokerKeytoolCertificateGenerator.generateSelfSignedCertificateEntry("test@kroxylicious.io", clientEndpoint.getConnect().getHost(), "Dev",
                        "Kroxylicious.io", null,
                        null,
                        "US");
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

        putConfig(server, "offsets.topic.replication.factor", ONE_CONFIG);
        // 1 partition for the __consumer_offsets_ topic should be enough
        putConfig(server, "offsets.topic.num.partitions", ONE_CONFIG);
        // 1 partition for the __transaction_state_ topic should be enough
        putConfig(server, "transaction.state.log.replication.factor", ONE_CONFIG);
        putConfig(server, "transaction.state.log.min.isr", ONE_CONFIG);
        // Disable delay during every re-balance
        putConfig(server, "group.initial.rebalance.delay.ms", Integer.toString(0));

        // The test harness doesn't rely upon Kafka JMX metrics (and probably won't because JMX isn't supported by
        // the kafka native). Registering/Unregistering the mbeans is time-consuming so we disable it.
        putConfig(server, "metrics.jmx.exclude", ".*");

        return new ConfigHolder(server, clientEndpoint.getConnect().getPort(), anonEndpoint.getConnect().getPort(),
                clientEndpoint.connectAddress(), nodeId, kafkaKraftClusterId);
    }

    private static void putConfig(Properties server, String key, String value) {
        var orig = server.put(key, value);
        if (orig != null) {
            throw new RuntimeException("Cannot override broker config '" + key + "=" + value + "' with new value " + orig);
        }
    }

    /**
     * Generates client connection config to connect to the anonymous listeners within the cluster. Thus bypassing all authentication mechanisms.
     *
     * @param bootstrapServers the bootstrap servers
     * @return the anon connect config for cluster
     */
    public Map<String, Object> getAnonConnectConfigForCluster(String bootstrapServers) {
        return getConnectConfigForCluster(bootstrapServers, null, null, null, null);
    }

    /**
     * Generates client connection config to connect to the cluster via the supplied bootstrap address.
     *
     * @param bootstrapServers the bootstrap servers
     * @return the connect config for cluster
     */
    public Map<String, Object> getConnectConfigForCluster(String bootstrapServers) {
        if (saslMechanism != null) {
            Map<String, String> users = getUsers();
            if (!users.isEmpty()) {
                Map.Entry<String, String> first = users.entrySet().iterator().next();
                return getConnectConfigForCluster(bootstrapServers, first.getKey(), first.getValue(), getSecurityProtocol(), getSaslMechanism());
            }
            else {
                return getConnectConfigForCluster(bootstrapServers, null, null, getSecurityProtocol(), getSaslMechanism());
            }
        }
        else {
            return getConnectConfigForCluster(bootstrapServers, null, null, getSecurityProtocol(), getSaslMechanism());
        }
    }

    /**
     * Generates client connection config to connect to the cluster via the supplied bootstrap address and user credentials.
     *
     * @param bootstrapServers the bootstrap servers
     * @param user the user
     * @param password the password
     * @return the connect config for cluster
     */
    public Map<String, Object> getConnectConfigForCluster(String bootstrapServers, String user, String password) {
        return getConnectConfigForCluster(bootstrapServers, user, password, getSecurityProtocol(), getSaslMechanism());
    }

    /**
     * Generates client connection config to connect to the cluster via the supplied bootstrap address and authentication configuration.
     *
     * @param bootstrapServers the bootstrap servers
     * @param user the user
     * @param password the password
     * @param securityProtocol the security protocol
     * @param saslMechanism the sasl mechanism
     * @return the connect config for cluster
     */
    public Map<String, Object> getConnectConfigForCluster(String bootstrapServers, String user, String password, String securityProtocol, String saslMechanism) {
        Map<String, Object> kafkaConfig = new HashMap<>();

        if (securityProtocol != null) {
            kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

            if (securityProtocol.contains("SSL")) {
                String clientTrustStoreFilePath;
                String clientTrustStorePassword;
                if (clientKeytoolCertificateGenerator != null) {
                    if (Path.of(clientKeytoolCertificateGenerator.getKeyStoreLocation()).toFile().exists()) {
                        // SSL client auth case
                        kafkaConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientKeytoolCertificateGenerator.getKeyStoreLocation());
                        kafkaConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientKeytoolCertificateGenerator.getPassword());
                        kafkaConfig.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, clientKeytoolCertificateGenerator.getPassword());
                    }
                    try {
                        clientKeytoolCertificateGenerator.generateTrustStore(brokerKeytoolCertificateGenerator.getCertFilePath(), "client");
                    }
                    catch (GeneralSecurityException | IOException e) {
                        throw new RuntimeException(e);
                    }
                    clientTrustStoreFilePath = clientKeytoolCertificateGenerator.getTrustStoreLocation();
                    clientTrustStorePassword = clientKeytoolCertificateGenerator.getPassword();
                }
                else {
                    Path clientTrustStore;
                    try {
                        Path certsDirectory = Files.createTempDirectory("kafkaClient");
                        clientTrustStore = Paths.get(certsDirectory.toAbsolutePath().toString(), "kafka.truststore.jks");
                        certsDirectory.toFile().deleteOnExit();
                        clientTrustStore.toFile().deleteOnExit();
                        brokerKeytoolCertificateGenerator.generateTrustStore(brokerKeytoolCertificateGenerator.getCertFilePath(), "client",
                                clientTrustStore.toAbsolutePath().toString());
                    }
                    catch (GeneralSecurityException | IOException e) {
                        throw new RuntimeException(e);
                    }
                    clientTrustStoreFilePath = clientTrustStore.toAbsolutePath().toString();
                    clientTrustStorePassword = brokerKeytoolCertificateGenerator.getPassword();
                }
                kafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStoreFilePath);
                kafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, clientTrustStorePassword);
            }
        }

        if (saslMechanism != null) {
            if (securityProtocol == null) {
                kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
            }
            kafkaConfig.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

            if ("PLAIN".equals(saslMechanism)) {
                if (user != null && password != null) {
                    kafkaConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                            String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                                    user, password));
                }
            }
            else {
                throw new IllegalStateException(String.format("unsupported SASL mechanism %s", saslMechanism));
            }
        }

        kafkaConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return kafkaConfig;
    }

    /**
     * Is the cluster coppering using Kraft Controller nodes.
     *
     * @return true if kraft mode is used, false otherwise
     */
    public boolean isKraftMode() {
        return this.getKraftMode() == null || this.getKraftMode();
    }

    /**
     * Cluster id string.
     *
     * @return the id
     */
    public String clusterId() {
        return isKraftMode() ? kafkaKraftClusterId : null;
    }

    /**
     * The type Config holder.
     */
    @Builder
    @Getter
    public static class ConfigHolder {
        private final Properties properties;
        private final Integer externalPort;
        private final Integer anonPort;
        private final String endpoint;
        private final int brokerNum;
        private final String kafkaKraftClusterId;

        /**
         * Instantiates a new Config holder.
         *
         * @param properties the properties
         * @param externalPort the external port
         * @param anonPort the anon port
         * @param endpoint the endpoint
         * @param brokerNum the broker num
         * @param kafkaKraftClusterId the kafka kraft cluster id
         */
        public ConfigHolder(Properties properties, Integer externalPort, Integer anonPort, String endpoint, int brokerNum, String kafkaKraftClusterId) {
            this.properties = properties;
            this.externalPort = externalPort;
            this.anonPort = anonPort;
            this.endpoint = endpoint;
            this.brokerNum = brokerNum;
            this.kafkaKraftClusterId = kafkaKraftClusterId;
        }
    }

    /**
     * The interface Kafka endpoints.
     */
    public interface KafkaEndpoints {

        /**
         * Enumeration of kafka listeners used by the test harness.
         */
        enum Listener {
            /**
             * used for communications to/from consumers/producers optionally with authentication
             */
            EXTERNAL,
            /**
             * used for communications to/from consumers/producers without authentication primarily for the extension to validate the cluster
             */
            ANON,
            /**
             * used for inter-broker communications (always no auth)
             */
            INTERNAL,
            /**
             * used for inter-broker controller communications (kraft - always no auth)
             */
            CONTROLLER
        }

        /**
         * The type Endpoint pair.
         */
        @Builder
        @Getter
        class EndpointPair {
            private final Endpoint bind;
            private final Endpoint connect;

            /**
             * Instantiates a new Endpoint pair.
             *
             * @param bind the bind
             * @param connect the endpoint
             */
            public EndpointPair(Endpoint bind, Endpoint connect) {
                this.bind = bind;
                this.connect = connect;
            }

            /**
             * Connect address string.
             *
             * @return the address
             */
            public String connectAddress() {
                return String.format("%s:%d", connect.host, connect.port);
            }

            /**
             * Listen address string.
             *
             * @return the listen address
             */
            public String listenAddress() {
                return String.format("//%s:%d", bind.host, bind.port);
            }

            /**
             * Advertised address string.
             *
             * @return the advertise address
             */
            public String advertisedAddress() {
                return String.format("//%s:%d", connect.host, connect.port);
            }
        }

        /**
         * The type Endpoint.
         */
        @Builder
        @Getter
        class Endpoint {
            private final String host;
            private final int port;

            /**
             * Instantiates a new Endpoint.
             *
             * @param host the host
             * @param port the port
             */
            public Endpoint(String host, int port) {
                this.host = host;
                this.port = port;
            }

            @Override
            public String toString() {
                return String.format("//%s:%d", host, port);
            }
        }

        /**
         * Gets the endpoint for the given listener and brokerId.
         *
         * @param listener listener
         * @param nodeId kafka <code>node.id</code>
         * @return endpoint poir.
         */
        EndpointPair getEndpointPair(Listener listener, int nodeId);

    }
}
