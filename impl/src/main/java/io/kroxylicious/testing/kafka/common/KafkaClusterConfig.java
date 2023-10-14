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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.TestInfo;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

/**
 * The Kafka cluster config class.
 */
@Builder(toBuilder = true)
@Getter
@ToString
public class KafkaClusterConfig {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterConfig.class.getName());
    public static final String BROKER_ROLE = "broker";
    public static final String CONTROLLER_ROLE = "controller";

    public static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
    public static final String EXTERNAL_LISTENER_NAME = "EXTERNAL";
    public static final String INTERNAL_LISTENER_NAME = "INTERNAL";
    public static final String ANON_LISTENER_NAME = "ANON";

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
     * @param testInfo information about the test execution context.
     * @return the kafka cluster config
     */
    public static KafkaClusterConfig fromConstraints(List<Annotation> annotations, TestInfo testInfo) {
        var builder = builder();
        builder.testInfo(testInfo);
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

}
