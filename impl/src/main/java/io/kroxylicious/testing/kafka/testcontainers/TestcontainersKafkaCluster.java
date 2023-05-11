/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;

import lombok.SneakyThrows;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.ListeningSocketPreallocator;

import static io.kroxylicious.testing.kafka.common.Utils.awaitExpectedBrokerCountInClusterViaTopic;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class TestcontainersKafkaCluster implements Startable, KafkaCluster {

    private static final System.Logger LOGGER = System.getLogger(TestcontainersKafkaCluster.class.getName());
    /**
     * The constant CLIENT_PORT.
     */
    public static final int CLIENT_PORT = 9093;
    /**
     * The constant ANON_PORT.
     */
    public static final int ANON_PORT = 9094;
    private static final int INTER_BROKER_PORT = 9092;
    private static final int CONTROLLER_PORT = 9091;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String QUAY_KAFKA_IMAGE_REPO = "quay.io/ogunalp/kafka-native";
    private static final String QUAY_ZOOKEEPER_IMAGE_REPO = "quay.io/ogunalp/zookeeper-native";
    private static final int CONTAINER_STARTUP_ATTEMPTS = 3;
    private static DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse(QUAY_KAFKA_IMAGE_REPO + ":latest-snapshot");
    private static DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse(QUAY_ZOOKEEPER_IMAGE_REPO + ":latest-snapshot");
    private static final int READY_TIMEOUT_SECONDS = 120;
    private final DockerImageName kafkaImage;
    private final DockerImageName zookeeperImage;
    private final KafkaClusterConfig clusterConfig;
    private final Network network = Network.newNetwork();
    private final ZookeeperContainer zookeeper;
    private final Collection<KafkaContainer> brokers;

    static {
        if (!System.getenv().containsKey("TESTCONTAINERS_RYUK_DISABLED")) {
            LOGGER.log(Level.WARNING,
                    "As per https://github.com/containers/podman/issues/7927#issuecomment-731525556 if using podman, set env var TESTCONTAINERS_RYUK_DISABLED=true");
        }
    }

    private final KafkaClusterConfig.KafkaEndpoints kafkaEndpoints;
    private List<ServerSocket> clientPorts;
    private List<ServerSocket> anonPorts;

    /**
     * Instantiates a new Testcontainers kafka cluster.
     *
     * @param clusterConfig the cluster config
     */
    public TestcontainersKafkaCluster(KafkaClusterConfig clusterConfig) {
        this(null, null, clusterConfig);
    }

    /**
     * Instantiates a new Testcontainers kafka cluster.
     *
     * @param kafkaImage the kafka image
     * @param zookeeperImage the zookeeper image
     * @param clusterConfig the cluster config
     */
    public TestcontainersKafkaCluster(DockerImageName kafkaImage, DockerImageName zookeeperImage, KafkaClusterConfig clusterConfig) {
        setDefaultKafkaImage(clusterConfig.getKafkaVersion());

        this.kafkaImage = Optional.ofNullable(kafkaImage).orElse(DEFAULT_KAFKA_IMAGE);
        this.zookeeperImage = Optional.ofNullable(zookeeperImage).orElse(DEFAULT_ZOOKEEPER_IMAGE);
        this.clusterConfig = clusterConfig;

        var name = Optional.ofNullable(clusterConfig.getTestInfo())
                .map(TestInfo::getDisplayName)
                .map(s -> s.replaceFirst("\\(\\)$", ""))
                .map(s -> String.format("%s.%s", s, OffsetDateTime.now()))
                .orElse(null);

        if (this.clusterConfig.isKraftMode()) {
            this.zookeeper = null;
        }
        else {
            this.zookeeper = new ZookeeperContainer(this.zookeeperImage)
                    .withName(name)
                    .withNetwork(network)
                    .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.zookeeper logging too
                    .withNetworkAliases("zookeeper");
        }

        try (var preallocator = new ListeningSocketPreallocator()) {
            clientPorts = preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum());
            anonPorts = preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum());
        }

        kafkaEndpoints = new KafkaClusterConfig.KafkaEndpoints() {
            @Override
            public EndpointPair getClientEndpoint(int brokerId) {
                return buildExposedEndpoint(brokerId, CLIENT_PORT, clientPorts);
            }

            @Override
            public EndpointPair getAnonEndpoint(int brokerId) {
                return buildExposedEndpoint(brokerId, ANON_PORT, anonPorts);
            }

            @Override
            public EndpointPair getInterBrokerEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", INTER_BROKER_PORT))
                        .connect(new Endpoint(String.format("broker-%d", brokerId), INTER_BROKER_PORT)).build();
            }

            @Override
            public EndpointPair getControllerEndpoint(int brokerId) {
                if (clusterConfig.isKraftMode()) {
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", CONTROLLER_PORT))
                            .connect(new Endpoint(String.format("broker-%d", brokerId), CONTROLLER_PORT)).build();
                }
                else {
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", ZOOKEEPER_PORT)).connect(new Endpoint("zookeeper", ZOOKEEPER_PORT)).build();
                }
            }

            private EndpointPair buildExposedEndpoint(int brokerId, int internalPort, List<ServerSocket> externalPortRange) {
                return EndpointPair.builder()
                        .bind(new Endpoint("0.0.0.0", internalPort))
                        .connect(new Endpoint("localhost", externalPortRange.get(brokerId).getLocalPort()))
                        .build();
            }
        };

        Supplier<KafkaClusterConfig.KafkaEndpoints> endPointConfigSupplier = () -> kafkaEndpoints;
        Supplier<KafkaClusterConfig.KafkaEndpoints.Endpoint> zookeeperEndpointSupplier = () -> new KafkaClusterConfig.KafkaEndpoints.Endpoint("zookeeper",
                TestcontainersKafkaCluster.ZOOKEEPER_PORT);
        this.brokers = clusterConfig.getBrokerConfigs(endPointConfigSupplier).map(holder -> {
            String netAlias = "broker-" + holder.getBrokerNum();
            KafkaContainer kafkaContainer = new KafkaContainer(this.kafkaImage)
                    .withName(name)
                    .withNetwork(this.network)
                    .withNetworkAliases(netAlias);

            copyHostKeyStoreToContainer(kafkaContainer, holder.getProperties(), SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            copyHostKeyStoreToContainer(kafkaContainer, holder.getProperties(), SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);

            kafkaContainer
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.kafka logging too
                    .withEnv("SERVER_PROPERTIES_FILE", "/cnf/server.properties")
                    .withEnv("SERVER_CLUSTER_ID", holder.getKafkaKraftClusterId())
                    .withCopyToContainer(Transferable.of(propertiesToBytes(holder.getProperties()), 0644), "/cnf/server.properties")
                    .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                    .withStartupTimeout(Duration.ofMinutes(2));
            kafkaContainer.addFixedExposedPort(holder.getExternalPort(), CLIENT_PORT);
            kafkaContainer.addFixedExposedPort(holder.getAnonPort(), ANON_PORT);

            if (!this.clusterConfig.isKraftMode()) {
                kafkaContainer.dependsOn(this.zookeeper);
            }
            return kafkaContainer;
        }).collect(Collectors.toList());
    }

    private void setDefaultKafkaImage(String kafkaVersion) {
        String kafkaVersionTag = (kafkaVersion == null || kafkaVersion.equals("latest")) ? "latest" : "latest-kafka-" + kafkaVersion;

        DEFAULT_KAFKA_IMAGE = DockerImageName.parse(QUAY_KAFKA_IMAGE_REPO + ":" + kafkaVersionTag);
        DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse(QUAY_ZOOKEEPER_IMAGE_REPO + ":" + kafkaVersionTag);
    }

    private static void copyHostKeyStoreToContainer(KafkaContainer container, Properties properties, String key) {
        if (properties.get(key) != null) {
            try {
                var hostPath = Path.of(String.valueOf(properties.get(key)));
                var containerPath = Path.of("/cnf", hostPath.getFileName().toString());
                properties.put(key, containerPath.toString());
                container.withCopyToContainer(Transferable.of(Files.readAllBytes(hostPath), 0644), containerPath.toString());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private byte[] propertiesToBytes(Properties properties) {
        try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
            properties.store(byteArrayOutputStream, "server.properties");
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getBootstrapServers() {
        return clusterConfig.buildClientBootstrapServers(kafkaEndpoints);
    }

    /**
     * Gets kafka version.
     *
     * @return the kafka version
     */
    public String getKafkaVersion() {
        return kafkaImage.getVersionPart();
    }

    private Stream<GenericContainer<?>> allContainers() {
        return Stream.concat(
                this.brokers.stream(),
                Stream.ofNullable(this.zookeeper));
    }

    @Override
    @SneakyThrows
    public void start() {
        try {
            if (zookeeper != null) {
                zookeeper.start();
            }
            Startables.deepStart(brokers.stream()).get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            awaitExpectedBrokerCountInClusterViaTopic(clusterConfig.getAnonConnectConfigForCluster(kafkaEndpoints), READY_TIMEOUT_SECONDS, TimeUnit.SECONDS,
                    clusterConfig.getBrokersNum());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            stop();
            throw new RuntimeException("startup failed or timed out", e);
        }
    }

    @Override
    public void close() {
        this.stop();
    }

    @Override
    public int getNumOfBrokers() {
        return clusterConfig.getBrokersNum();
    }

    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }

    @Override
    public String getClusterId() {
        return clusterConfig.clusterId();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers());
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String user, String password) {
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers(), user, password);
    }

    /**
     * The type Kafka container.
     */
    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort to for use.
    public static class KafkaContainer extends LoggingGenericContainer<KafkaContainer> {

        /**
         * Instantiates a new Kafka container.
         *
         * @param dockerImageName the docker image name
         */
        public KafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }

    }

    /**
     * The type Zookeeper container.
     */
    public static class ZookeeperContainer extends LoggingGenericContainer<ZookeeperContainer> {
        /**
         * Instantiates a new Zookeeper container.
         *
         * @param zookeeperImage the zookeeper image
         */
        public ZookeeperContainer(DockerImageName zookeeperImage) {
            super(zookeeperImage);
        }
    }

    /**
     * The type Logging generic container.
     *
     * @param <C>  the type parameter
     */
    public static class LoggingGenericContainer<C extends GenericContainer<C>>
            extends GenericContainer<C> {
        private static final String CONTAINER_LOGS_DIR = "container.logs.dir";
        private String name;

        /**
         * Instantiates a new Logging generic container.
         *
         * @param dockerImageName the docker image name
         */
        public LoggingGenericContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            super.containerIsStarting(containerInfo);

            Optional.ofNullable(System.getProperty(CONTAINER_LOGS_DIR)).ifPresent(logDir -> {
                var target = Path.of(logDir);
                if (name != null) {
                    target = target.resolve(name);
                }
                target = target.resolve(String.format("%s.%s.%s", getContainerName().replaceFirst(File.separator, ""), getContainerId(), "log"));
                target.getParent().toFile().mkdirs();
                try {
                    var writer = new FileWriter(target.toFile());
                    super.followOutput(outputFrame -> {
                        try {
                            if (outputFrame.equals(OutputFrame.END)) {
                                writer.close();
                            }
                            else {
                                writer.write(outputFrame.getUtf8String());
                            }
                        }
                        catch (IOException e) {
                            // ignore
                        }
                    });
                }
                catch (IOException e) {
                    logger().warn("Failed to create container log file: {}", target);
                }
            });

        }

        /**
         * With name logging generic container.
         *
         * @param name the name
         * @return the logging generic container
         */
        public LoggingGenericContainer<C> withName(String name) {
            this.name = name;
            return this;
        }
    }

}
