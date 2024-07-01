/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.TestInfo;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import kafka.server.KafkaConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig.KafkaEndpoints.Listener;

import static java.util.Locale.ROOT;

/**
 * The Kafka cluster config class.
 */
@Builder(toBuilder = true)
@Getter
@ToString
public class KafkaClusterConfig {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterConfig.class.getName());
    private static final String ONE_CONFIG = Integer.toString(1);
    private static final AtomicBoolean DEPRECATED_SASL_PLAIN_AUTH_USE_REPORTED = new AtomicBoolean();

    public static final String BROKER_ROLE = "broker";
    public static final String CONTROLLER_ROLE = "controller";

    public static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
    public static final String EXTERNAL_LISTENER_NAME = "EXTERNAL";
    public static final String INTERNAL_LISTENER_NAME = "INTERNAL";
    public static final String ANON_LISTENER_NAME = "ANON";

    private static final String SCRAM_SHA_SASL_MECHANISM_PREFIX = "SCRAM-SHA-";
    private static final String PLAIN_SASL_MECHANISM_NAME = "PLAIN";
    private static final String OAUTHBEARER_SASL_MECHANISM_NAME = "OAUTHBEARER";
    private static final String SCRAM_256_SASL_MECHANISM_NAME = "SCRAM-SHA-256";
    private static final String SCRAM_512_SASL_MECHANISM_NAME = "SCRAM-SHA-512";

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
     * Defaults to the version available on the classpath.
     * <br/>
     * The value is only used when execMode is {@link KafkaClusterExecutionMode#CONTAINER}.
     */
    @Builder.Default
    @lombok.NonNull
    private String kafkaVersion = detectKafkaVersionFromClasspath();

    /**
     * name of SASL mechanism to be configured on kafka for the external listener, if null, anonymous communication
     * will be used.
     */
    private final String saslMechanism;

    /**
     * name of login module that will be used to for client and broker. if null, the login module will be
     * derived from the saslMechanism.
     */
    @Nullable
    private String loginModule;

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
    private final Map<String, String> jaasServerOptions;

    @Singular
    private final Map<String, String> jaasClientOptions;

    @Singular
    private final Map<String, String> brokerConfigs;

    @SuppressWarnings("java:S5738") // silence warnings about the use of deprecated code
    private static final Set<Class<? extends Annotation>> SUPPORTED_CONSTRAINTS = Set.of(
            ClusterId.class,
            BrokerCluster.class,
            BrokerConfig.class,
            BrokerConfig.List.class,
            KRaftCluster.class,
            Tls.class,
            SaslMechanism.class,
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
    @SuppressWarnings("java:S5738") // silence warnings about the use of deprecated annotations
    public static KafkaClusterConfig fromConstraints(List<Annotation> annotations, TestInfo testInfo) {
        var builder = builder();
        builder.testInfo(testInfo);
        builder.brokersNum(1);
        var tls = false;
        var useSasl = false;
        var saslUsers = Optional.<Map<String, String>> empty();
        var deprecatedSaslUsers = Optional.<Map<String, String>> empty();

        for (Annotation annotation : annotations) {
            if (annotation instanceof BrokerCluster brokerCluster) {
                builder.brokersNum(brokerCluster.numBrokers());
            }
            else if (annotation instanceof KRaftCluster kRaftCluster) {
                builder.kraftMode(true);
                builder.kraftControllers(kRaftCluster.numControllers());
            }
            else if (annotation instanceof ZooKeeperCluster) {
                builder.kraftMode(false);
            }
            else if (annotation instanceof Tls) {
                tls = true;
                processTls(builder::brokerKeytoolCertificateGenerator);
            }
            else if (annotation instanceof ClusterId clusterId) {
                builder.kafkaKraftClusterId(clusterId.value());
            }
            else if (annotation instanceof Version version) {
                builder.kafkaVersion(version.value());
            }
            else if (annotation instanceof SaslMechanism mechanism) {
                useSasl = true;
                builder.saslMechanism(Optional.ofNullable(mechanism.value()).orElse(PLAIN_SASL_MECHANISM_NAME));
                var principals = Optional.ofNullable(mechanism.principals()).orElse(new SaslMechanism.Principal[]{});
                saslUsers = Optional.of(Arrays.stream(principals)
                        .collect(Collectors.toMap(SaslMechanism.Principal::user, SaslMechanism.Principal::password)));
            }
            else if (annotation instanceof SaslPlainAuth || annotation instanceof SaslPlainAuth.List) {
                deprecatedSaslUsers = processDeprecatedSaslUserAnnotations(annotation);
            }
            else if (annotation instanceof BrokerConfig || annotation instanceof BrokerConfig.List) {
                processBrokerConfigs(annotation, builder::brokerConfig);
            }
            else {
                throw new IllegalArgumentException("unexpected constraint annotation " + annotation.getClass());
            }
        }

        if (deprecatedSaslUsers.isPresent()) {
            if (!DEPRECATED_SASL_PLAIN_AUTH_USE_REPORTED.compareAndExchange(false, true)) {
                LOGGER.log(System.Logger.Level.WARNING, "Use of deprecated SaslPlainAuth annotation, use SaslUser instead.");
            }
            if (useSasl) {
                throw new IllegalArgumentException("Cannot use deprecated SaslPlainAuth with SaslMechanism.");
            }
            else {
                builder.saslMechanism(PLAIN_SASL_MECHANISM_NAME);
                deprecatedSaslUsers.ifPresent(builder::users);
                useSasl = true;
            }
        }
        else if (saslUsers.isPresent()) {
            saslUsers.ifPresent(builder::users);
        }

        builder.securityProtocol(determineSecurityProtocol(useSasl, tls));
        return builder.build();
    }

    private static void processTls(Consumer<KeytoolCertificateGenerator> consumer) {
        try {
            consumer.accept(new KeytoolCertificateGenerator());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create broker certificate", e);
        }
    }

    @SuppressWarnings("java:S5738") // silence warnings about the use of deprecated code
    private static Optional<Map<String, String>> processDeprecatedSaslUserAnnotations(Annotation annotation) {
        if (annotation instanceof SaslPlainAuth.List saslPlainAuthList) {
            return Optional.of(Arrays.stream(saslPlainAuthList.value())
                    .collect(Collectors.toMap(SaslPlainAuth::user, SaslPlainAuth::password)));
        }
        else if (annotation instanceof SaslPlainAuth saslPlainAuth) {
            return Optional.of(Map.of(saslPlainAuth.user(), saslPlainAuth.password()));
        }
        return Optional.empty();
    }

    private static void processBrokerConfigs(Annotation annotation, BiConsumer<String, String> consumer) {
        if (annotation instanceof BrokerConfig.List brokerConfigList) {
            for (var config : brokerConfigList.value()) {
                consumer.accept(config.name(), config.value());
            }
        }
        else if (annotation instanceof BrokerConfig brokerConfig) {
            consumer.accept(brokerConfig.name(), brokerConfig.value());
        }
    }

    private static String determineSecurityProtocol(boolean useSasl, boolean tls) {
        return (useSasl ? "SASL_" : "") + (tls ? "SSL" : "PLAINTEXT");
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
        final int nodeCount = Math.max(brokersNum, kraftControllers);
        for (int brokerNum = 0; brokerNum < nodeCount; brokerNum++) {
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
    @NonNull
    public ConfigHolder generateConfigForSpecificNode(KafkaEndpoints kafkaEndpoints, int nodeId) {
        final var role = determineRole(nodeId);
        Properties nodeConfiguration = new Properties();
        nodeConfiguration.putAll(brokerConfigs);

        putConfig(nodeConfiguration, "broker.id", Integer.toString(nodeId));

        var protocolMap = new TreeMap<String, String>();
        var listeners = new TreeMap<String, String>();
        var advertisedListeners = new TreeMap<String, String>();
        var earlyStart = new TreeSet<String>();

        final ConfigHolder configHolder;
        if (role.contains(BROKER_ROLE)) {
            configHolder = configureBroker(kafkaEndpoints, nodeId, protocolMap, listeners, advertisedListeners, earlyStart, nodeConfiguration);
        }
        else {
            configHolder = configureController(kafkaEndpoints, nodeId, nodeConfiguration);
        }

        if (isKraftMode()) {
            configureKraftNode(kafkaEndpoints, nodeId, nodeConfiguration, protocolMap, listeners, earlyStart, role);
        }
        else {
            configureLegacyNode(kafkaEndpoints, nodeConfiguration);
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

        return configHolder;
    }

    @NonNull
    private ConfigHolder configureController(KafkaEndpoints kafkaEndpoints, int nodeId, Properties nodeConfiguration) {
        return new ConfigHolder(nodeConfiguration, null, null, null,
                nodeId, kafkaKraftClusterId);
    }

    @NonNull
    private ConfigHolder configureBroker(KafkaEndpoints kafkaEndpoints, int nodeId, TreeMap<String, String> protocolMap, TreeMap<String, String> listeners,
                                         TreeMap<String, String> advertisedListeners, TreeSet<String> earlyStart, Properties nodeConfiguration) {
        final ConfigHolder configHolder;
        var interBrokerEndpoint = kafkaEndpoints.getEndpointPair(Listener.INTERNAL, nodeId);
        var clientEndpoint = kafkaEndpoints.getEndpointPair(Listener.EXTERNAL, nodeId);
        var anonEndpoint = kafkaEndpoints.getEndpointPair(Listener.ANON, nodeId);

        // - EXTERNAL: used for communications to/from consumers/producers optionally with authentication
        // - ANON: used for communications to/from consumers/producers without authentication primarily for the extension to validate the cluster
        // - INTERNAL: used for inter-broker communications (always no auth)
        // - CONTROLLER: used for inter-broker controller communications (kraft - always no auth)

        var externalListenerTransport = securityProtocol == null ? SecurityProtocol.PLAINTEXT.name() : securityProtocol;
        configureExternalListener(protocolMap, externalListenerTransport, listeners, clientEndpoint, advertisedListeners);
        configureInternalListener(protocolMap, listeners, interBrokerEndpoint, advertisedListeners, earlyStart, nodeConfiguration);
        configureAnonListener(protocolMap, listeners, anonEndpoint, advertisedListeners);
        configureTls(clientEndpoint, nodeConfiguration);
        putConfig(nodeConfiguration, "advertised.listeners",
                advertisedListeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
        configHolder = new ConfigHolder(nodeConfiguration, clientEndpoint.getConnect().getPort(), anonEndpoint.getConnect().getPort(),
                clientEndpoint.connectAddress(), nodeId, kafkaKraftClusterId);
        return configHolder;
    }

    @NonNull
    private String determineRole(int nodeId) {
        var roles = new ArrayList<String>();

        if (nodeId < brokersNum || isAdditionalNode(nodeId)) {
            roles.add(BROKER_ROLE);
        }
        if (nodeId < kraftControllers) {
            roles.add(CONTROLLER_ROLE);
        }
        return String.join(",", roles);
    }

    // additional nodes can only be added after the initial topology is generated.
    // Hence, it is safe to assume that a node is additional if it has a higherId than the initial topology would allow for.
    private boolean isAdditionalNode(int nodeId) {
        return nodeId >= Math.max(brokersNum, kraftControllers);
    }

    private void configureTls(KafkaEndpoints.EndpointPair clientEndpoint, Properties server) {
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
                        server.put("listener.name.%s.%s".formatted(EXTERNAL_LISTENER_NAME.toLowerCase(ROOT), BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG), "required");
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
        if (saslMechanism != null) {
            putConfig(server, "sasl.enabled.mechanisms", saslMechanism);

            var lm = Optional.ofNullable(loginModule).orElse(deriveLoginModuleFromSasl(saslMechanism));
            var serverOptions = Optional.ofNullable(jaasServerOptions).orElse(Map.of()).entrySet().stream();
            Stream<Map.Entry<String, String>> userOptions = Stream.empty();
            // Note: Scram users are added to the server at after startup.
            if (isSaslPlain()) {
                userOptions = Optional.ofNullable(users).orElse(Map.of()).entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> String.format("user_%s", e.getKey()), Map.Entry::getValue)).entrySet().stream();
            }

            var moduleOptions = Stream.concat(serverOptions, userOptions)
                    .map(e -> String.join("=", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(" "));

            var moduleConfig = String.format("%s required %s;", lm, moduleOptions);
            var configKey = String.format("listener.name.%s.%s.sasl.jaas.config", EXTERNAL_LISTENER_NAME.toLowerCase(ROOT), saslMechanism.toLowerCase(ROOT));

            putConfig(server, configKey, moduleConfig);
        }
    }

    private String deriveLoginModuleFromSasl(String saslMechanism) {
        switch (saslMechanism.toUpperCase(ROOT)) {
            case PLAIN_SASL_MECHANISM_NAME -> {
                return PlainLoginModule.class.getName();
            }
            case SCRAM_256_SASL_MECHANISM_NAME, SCRAM_512_SASL_MECHANISM_NAME -> {
                return ScramLoginModule.class.getName();
            }
            case OAUTHBEARER_SASL_MECHANISM_NAME -> {
                return OAuthBearerLoginModule.class.getName();
            }
            default -> throw new IllegalArgumentException("Cannot derive login module from saslMechanism %s".formatted(saslMechanism));
        }
    }

    private static void configureInternalListener(TreeMap<String, String> protocolMap, TreeMap<String, String> listeners, KafkaEndpoints.EndpointPair interBrokerEndpoint,
                                                  TreeMap<String, String> advertisedListeners, TreeSet<String> earlyStart, Properties server) {
        protocolMap.put(INTERNAL_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());
        listeners.put(INTERNAL_LISTENER_NAME, interBrokerEndpoint.listenAddress());
        advertisedListeners.put(INTERNAL_LISTENER_NAME, interBrokerEndpoint.advertisedAddress());
        earlyStart.add(INTERNAL_LISTENER_NAME);
        putConfig(server, "inter.broker.listener.name", INTERNAL_LISTENER_NAME);
    }

    private static void configureAnonListener(TreeMap<String, String> protocolMap, TreeMap<String, String> listeners, KafkaEndpoints.EndpointPair anonEndpoint,
                                              TreeMap<String, String> advertisedListeners) {
        protocolMap.put(ANON_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());
        listeners.put(ANON_LISTENER_NAME, anonEndpoint.listenAddress());
        advertisedListeners.put(ANON_LISTENER_NAME, anonEndpoint.advertisedAddress());
    }

    private static void configureExternalListener(TreeMap<String, String> protocolMap, String externalListenerTransport, TreeMap<String, String> listeners,
                                                  KafkaEndpoints.EndpointPair clientEndpoint, TreeMap<String, String> advertisedListeners) {
        protocolMap.put(EXTERNAL_LISTENER_NAME, externalListenerTransport);
        listeners.put(EXTERNAL_LISTENER_NAME, clientEndpoint.listenAddress());
        advertisedListeners.put(EXTERNAL_LISTENER_NAME, clientEndpoint.advertisedAddress());
    }

    private static void configureLegacyNode(KafkaEndpoints kafkaEndpoints, Properties server) {
        putConfig(server, "zookeeper.connect", kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, 0).connectAddress());
        putConfig(server, "zookeeper.sasl.enabled", "false");
        putConfig(server, "zookeeper.connection.timeout.ms", Long.toString(60000));
        putConfig(server, KafkaConfig.ZkSessionTimeoutMsProp(), Long.toString(6000));
    }

    private void configureKraftNode(KafkaEndpoints kafkaEndpoints, int nodeId, Properties nodeConfiguration, TreeMap<String, String> protocolMap,
                                    TreeMap<String, String> listeners,
                                    TreeSet<String> earlyStart,
                                    String role) {
        putConfig(nodeConfiguration, "node.id", Integer.toString(nodeId)); // Required by Kafka 3.3 onwards.

        var quorumVoters = IntStream.range(0, kraftControllers)
                .mapToObj(controllerId -> String.format("%d@//%s", controllerId, kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, controllerId).connectAddress()))
                .collect(Collectors.joining(","));
        putConfig(nodeConfiguration, "controller.quorum.voters", quorumVoters);
        putConfig(nodeConfiguration, "controller.listener.names", CONTROLLER_LISTENER_NAME);
        protocolMap.put(CONTROLLER_LISTENER_NAME, SecurityProtocol.PLAINTEXT.name());

        putConfig(nodeConfiguration, "process.roles", role);
        if (role.contains(CONTROLLER_ROLE)) {
            var controllerEndpoint = kafkaEndpoints.getEndpointPair(Listener.CONTROLLER, nodeId);
            final String bindAddress = controllerEndpoint.getBind().toString();
            listeners.put(CONTROLLER_LISTENER_NAME, bindAddress);
            earlyStart.add(CONTROLLER_LISTENER_NAME);
        }
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
            var externalUsers = getUsers();
            if (!externalUsers.isEmpty()) {
                Map.Entry<String, String> first = externalUsers.entrySet().iterator().next();
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
                buildSecurityProtocolConfig(kafkaConfig);
            }
        }

        if (saslMechanism != null) {
            buildSaslConnectConfig(kafkaConfig, user, password, securityProtocol, saslMechanism);
        }

        kafkaConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return kafkaConfig;
    }

    private void buildSecurityProtocolConfig(Map<String, Object> kafkaConfig) {
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

    private void buildSaslConnectConfig(Map<String, Object> kafkaConfig, String user, String password, String securityProtocol, String saslMechanism) {
        kafkaConfig.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        var lm = Optional.ofNullable(loginModule).orElse(deriveLoginModuleFromSasl(saslMechanism));
        if (securityProtocol == null) {
            kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        }

        var jaasOptions = new HashMap<>(jaasClientOptions == null ? Map.of() : jaasClientOptions);

        if (isSaslPlain() || isSaslScram()) {
            applyCredential(jaasOptions, "username", user);
            applyCredential(jaasOptions, "password", password);
        }

        var moduleOptions = jaasOptions.entrySet().stream()
                .map(e -> String.join("=", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));

        kafkaConfig.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("%s required %s;", lm, moduleOptions));
    }

    private void applyCredential(HashMap<String, String> jaasOptions, String credentialKey, String credentialValue) {
        jaasOptions.computeIfAbsent(credentialKey, k -> credentialValue);
        if (!jaasOptions.containsKey(credentialKey)) {
            LOGGER.log(System.Logger.Level.WARNING, "No {} value specified for SASL authentication", credentialKey);
        }
    }

    private boolean isSaslPlain() {
        return this.saslMechanism != null && this.saslMechanism.toUpperCase(ROOT).equals(PLAIN_SASL_MECHANISM_NAME);
    }

    public boolean isSaslScram() {
        return this.saslMechanism != null && this.saslMechanism.toUpperCase(ROOT).startsWith(SCRAM_SHA_SASL_MECHANISM_PREFIX);
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

    private static String detectKafkaVersionFromClasspath() {
        var version = AppInfoParser.getVersion();
        var appInfoParserUnknown = "unknown"; // Defined by AppInfoParser.DEFAULT_VALUE. Symbol is package-private.
        return version == null || version.equals(appInfoParserUnknown) ? Version.LATEST_RELEASE : version;
    }

    /**
     * The type Config holder.
     */
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
         * @param properties          the properties
         * @param externalPort        the external port
         * @param anonPort            the anon port
         * @param endpoint            the endpoint
         * @param brokerNum           the broker num
         * @param kafkaKraftClusterId the kafka kraft cluster id
         */
        @Builder
        public ConfigHolder(Properties properties, Integer externalPort, Integer anonPort, String endpoint, int brokerNum, String kafkaKraftClusterId) {
            this.properties = properties;
            this.externalPort = externalPort;
            this.anonPort = anonPort;
            this.endpoint = endpoint;
            this.brokerNum = brokerNum;
            this.kafkaKraftClusterId = kafkaKraftClusterId;
        }

        private String getRoles() {
            return properties.getProperty("process.roles", BROKER_ROLE);
        }

        public boolean isBroker() {
            return getRoles().contains(BROKER_ROLE);
        }

        public boolean isController() {
            return getRoles().contains(CONTROLLER_ROLE);
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
