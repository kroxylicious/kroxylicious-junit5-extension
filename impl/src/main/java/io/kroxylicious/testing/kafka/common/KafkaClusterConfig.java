/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.TestInfo;

import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig.KafkaEndpoints.Endpoint;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@ToString
public class KafkaClusterConfig {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterConfig.class.getName());

    private TestInfo testInfo;
    private KeytoolCertificateGenerator keytoolCertificateGenerator;

    /**
     * specifies the cluster execution mode.
     */
    private final KafkaClusterExecutionMode execMode;

    /**
     * if true, the cluster will be brought up in Kraft-mode
     */
    private final Boolean kraftMode;

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
            ZooKeeperCluster.class);

    public static boolean supportsConstraint(Class<? extends Annotation> annotation) {
        return SUPPORTED_CONSTRAINTS.contains(annotation);
    }

    public static KafkaClusterConfig fromConstraints(List<Annotation> annotations) {
        System.Logger logger = System.getLogger(KafkaClusterProvisioningStrategy.class.getName());
        var builder = builder();
        builder.brokersNum(1);
        boolean sasl = false;
        boolean tls = false;
        for (Annotation annotation : annotations) {
            if (annotation instanceof BrokerCluster) {
                builder.brokersNum(((BrokerCluster) annotation).numBrokers());
            }
            if (annotation instanceof KRaftCluster) {
                builder.kraftMode(true);
                builder.kraftControllers(((KRaftCluster) annotation).numControllers());
            }
            if (annotation instanceof ZooKeeperCluster) {
                builder.kraftMode(false);
            }
            if (annotation instanceof Tls) {
                tls = true;
                builder.keytoolCertificateGenerator(new KeytoolCertificateGenerator());
            }
            if (annotation instanceof SaslPlainAuth) {
                builder.saslMechanism("PLAIN");
                sasl = true;
                builder.users(Arrays.stream(((SaslPlainAuth) annotation).value())
                        .collect(Collectors.toMap(
                                SaslPlainAuth.UserPassword::user,
                                SaslPlainAuth.UserPassword::password)));
            }
            if (annotation instanceof ClusterId) {
                builder.kafkaKraftClusterId(((ClusterId) annotation).value());
            }
            if (annotation instanceof BrokerConfig.List) {
                for (var config : ((BrokerConfig.List) annotation).value()) {
                    builder.brokerConfig(config.name(), config.value());
                }
            }
            else if (annotation instanceof BrokerConfig) {
                builder.brokerConfig(((BrokerConfig) annotation).name(), ((BrokerConfig) annotation).value());
            }
        }
        builder.securityProtocol((sasl ? "SASL_" : "") + (tls ? "SSL" : "PLAINTEXT"));
        KafkaClusterConfig clusterConfig = builder.build();
        return clusterConfig;
    }

    public Stream<ConfigHolder> getBrokerConfigs(Supplier<KafkaEndpoints> endPointConfigSupplier, Supplier<Endpoint> zookeeperEndpointSupplier) {
        List<ConfigHolder> properties = new ArrayList<>();
        KafkaEndpoints kafkaEndpoints = endPointConfigSupplier.get();
        for (int brokerNum = 0; brokerNum < brokersNum; brokerNum++) {
            Properties server = new Properties();
            server.putAll(brokerConfigs);

            putConfig(server, "broker.id", Integer.toString(brokerNum));

            var interBrokerEndpoint = kafkaEndpoints.getInterBrokerEndpoint(brokerNum);
            var clientEndpoint = kafkaEndpoints.getClientEndpoint(brokerNum);

            // - EXTERNAL: used for communications to/from consumers/producers
            // - INTERNAL: used for inter-broker communications (always no auth)
            // - CONTROLLER: used for inter-broker controller communications (kraft - always no auth)

            var externalListenerTransport = securityProtocol == null ? SecurityProtocol.PLAINTEXT.name() : securityProtocol;

            var protocolMap = new TreeMap<String, String>();
            var listeners = new TreeMap<String, String>();
            var advertisedListeners = new TreeMap<String, String>();

            protocolMap.put("EXTERNAL", externalListenerTransport);
            listeners.put("EXTERNAL", clientEndpoint.getBind().toString());
            advertisedListeners.put("EXTERNAL", clientEndpoint.getConnect().toString());

            protocolMap.put("INTERNAL", SecurityProtocol.PLAINTEXT.name());
            listeners.put("INTERNAL", interBrokerEndpoint.getBind().toString());
            advertisedListeners.put("INTERNAL", interBrokerEndpoint.getConnect().toString());
            putConfig(server, "inter.broker.listener.name", "INTERNAL");

            if (isKraftMode()) {
                putConfig(server, "node.id", Integer.toString(brokerNum)); // Required by Kafka 3.3 onwards.

                var controllerEndpoint = kafkaEndpoints.getControllerEndpoint(brokerNum);
                var quorumVoters = IntStream.range(0, kraftControllers)
                        .mapToObj(b -> String.format("%d@%s", b, kafkaEndpoints.getControllerEndpoint(b).getConnect().toString())).collect(Collectors.joining(","));
                putConfig(server, "controller.quorum.voters", quorumVoters);
                putConfig(server, "controller.listener.names", "CONTROLLER");
                protocolMap.put("CONTROLLER", SecurityProtocol.PLAINTEXT.name());

                if (brokerNum == 0) {
                    putConfig(server, "process.roles", "broker,controller");

                    listeners.put("CONTROLLER", controllerEndpoint.getBind().toString());
                }
                else {
                    putConfig(server, "process.roles", "broker");
                }
            }
            else {
                putConfig(server, "zookeeper.connect", String.format("%s:%d", zookeeperEndpointSupplier.get().getHost(), zookeeperEndpointSupplier.get().getPort()));
                putConfig(server, "zookeeper.sasl.enabled", "false");
                putConfig(server, "zookeeper.connection.timeout.ms", Long.toString(60000));
            }

            putConfig(server, "listener.security.protocol.map",
                    protocolMap.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            putConfig(server, "listeners", listeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            putConfig(server, "advertised.listeners", advertisedListeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            putConfig(server, "early.start.listeners", advertisedListeners.keySet().stream().map(Object::toString).collect(Collectors.joining(",")));

            if (saslMechanism != null) {
                putConfig(server, "sasl.enabled.mechanisms", saslMechanism);

                var saslPairs = new StringBuilder();

                Optional.of(users).orElse(Map.of()).forEach((key, value) -> {
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
                if (keytoolCertificateGenerator == null) {
                    throw new RuntimeException("keytoolCertificateGenerator needs to be initialized when calling KafkaClusterConfig");
                }
                try {
                    keytoolCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", clientEndpoint.getConnect().getHost(), "KI", "RedHat", null, null,
                            "US");
                }
                catch (GeneralSecurityException | IOException e) {
                    throw new RuntimeException(e);
                }
                server.put("ssl.client.auth", "required");
                server.put("ssl.truststore.location", keytoolCertificateGenerator.getCertLocation());
                server.put("ssl.truststore.password", keytoolCertificateGenerator.getPassword());
                server.put("ssl.keystore.location", keytoolCertificateGenerator.getCertLocation());
                server.put("ssl.keystore.password", keytoolCertificateGenerator.getPassword());
                server.put("ssl.key.password", keytoolCertificateGenerator.getPassword());
            }

            putConfig(server, "offsets.topic.replication.factor", Integer.toString(1));
            // 1 partition for the __consumer_offsets_ topic should be enough
            putConfig(server, "offsets.topic.num.partitions", Integer.toString(1));
            // Disable delay during every re-balance
            putConfig(server, "group.initial.rebalance.delay.ms", Integer.toString(0));

            properties.add(new ConfigHolder(server, clientEndpoint.getConnect().getPort(),
                    String.format("%s:%d", clientEndpoint.getConnect().getHost(), clientEndpoint.getConnect().getPort()), brokerNum, kafkaKraftClusterId));
        }

        return properties.stream();
    }

    private static void putConfig(Properties server, String key, String value) {
        var orig = server.put(key, value);
        if (orig != null) {
            throw new RuntimeException("Cannot override broker config '" + key + "=" + value + "' with new value " + orig);
        }
    }

    public Map<String, Object> getConnectConfigForCluster(String bootstrapServers) {
        if (saslMechanism != null) {
            Map<String, String> users = getUsers();
            if (!users.isEmpty()) {
                Map.Entry<String, String> first = users.entrySet().iterator().next();
                return getConnectConfigForCluster(bootstrapServers, first.getKey(), first.getKey());
            }
            else {
                return getConnectConfigForCluster(bootstrapServers, null, null);
            }
        }
        else {
            return getConnectConfigForCluster(bootstrapServers, null, null);
        }
    }

    public Map<String, Object> getConnectConfigForCluster(String bootstrapServers, String user, String password) {
        Map<String, Object> kafkaConfig = new HashMap<>();
        String saslMechanism = getSaslMechanism();
        String securityProtocol = getSecurityProtocol();

        if (securityProtocol != null) {
            kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

            if (securityProtocol.contains("SSL")) {
                kafkaConfig.put("ssl.truststore.location", keytoolCertificateGenerator.getCertLocation());
                kafkaConfig.put("ssl.truststore.password", keytoolCertificateGenerator.getPassword());
                if (securityProtocol.equals(SecurityProtocol.SSL.name())) {
                    kafkaConfig.put("ssl.keystore.location", keytoolCertificateGenerator.getCertLocation());
                    kafkaConfig.put("ssl.keystore.password", keytoolCertificateGenerator.getPassword());
                    kafkaConfig.put("ssl.key.password", keytoolCertificateGenerator.getPassword());
                }
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

    public boolean isKraftMode() {
        return this.getKraftMode() == null || this.getKraftMode();
    }

    public String clusterId() {
        return isKraftMode() ? kafkaKraftClusterId : null;
    }

    @Builder
    @Getter
    public static class ConfigHolder {
        private final Properties properties;
        private final Integer externalPort;
        private final String endpoint;
        private final int brokerNum;
        private final String kafkaKraftClusterId;
    }

    public interface KafkaEndpoints {

        @Builder
        @Getter
        class EndpointPair {
            private final Endpoint bind;
            private final Endpoint connect;
        }

        @Builder
        @Getter
        public class Endpoint {
            private final String host;
            private final int port;

            public Endpoint(String host, int port) {
                this.host = host;
                this.port = port;
            }

            @Override
            public String toString() {
                return String.format("//%s:%d", host, port);
            }
        }

        EndpointPair getInterBrokerEndpoint(int brokerId);

        EndpointPair getControllerEndpoint(int brokerId);

        EndpointPair getClientEndpoint(int brokerId);
    }
}
