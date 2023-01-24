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

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.Utils;
import lombok.SneakyThrows;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class TestcontainersKafkaCluster implements Startable, KafkaCluster {

    private static final System.Logger LOGGER = System.getLogger(TestcontainersKafkaCluster.class.getName());
    public static final int KAFKA_PORT = 9093;
    public static final int ZOOKEEPER_PORT = 2181;

    private static DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse("quay.io/ogunalp/kafka-native:latest-snapshot");
    private static DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse("quay.io/ogunalp/zookeeper-native:latest-snapshot");
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

    public TestcontainersKafkaCluster(KafkaClusterConfig clusterConfig) {
        this(null, null, clusterConfig);
    }

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
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.zookeeper logging too
                    .withNetworkAliases("zookeeper");
        }

        Supplier<KafkaClusterConfig.KafkaEndpoints> endPointConfigSupplier = () -> new KafkaClusterConfig.KafkaEndpoints() {
            final List<Integer> ports = Utils.preAllocateListeningPorts(clusterConfig.getBrokersNum()).collect(Collectors.toList());

            @Override
            public EndpointPair getClientEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9093)).connect(new Endpoint("localhost", ports.get(brokerId))).build();
            }

            @Override
            public EndpointPair getInterBrokerEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9092)).connect(new Endpoint(String.format("broker-%d", brokerId), 9092)).build();
            }

            @Override
            public EndpointPair getControllerEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9091)).connect(new Endpoint(String.format("broker-%d", brokerId), 9091)).build();
            }
        };
        Supplier<KafkaClusterConfig.KafkaEndpoints.Endpoint> zookeeperEndpointSupplier = () -> new KafkaClusterConfig.KafkaEndpoints.Endpoint("zookeeper",
                TestcontainersKafkaCluster.ZOOKEEPER_PORT);
        this.brokers = clusterConfig.getBrokerConfigs(endPointConfigSupplier, zookeeperEndpointSupplier).map(holder -> {
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
                    .withStartupTimeout(Duration.ofMinutes(2));
            kafkaContainer.addFixedExposedPort(holder.getExternalPort(), KAFKA_PORT);

            if (!this.clusterConfig.isKraftMode()) {
                kafkaContainer.dependsOn(this.zookeeper);
            }
            return kafkaContainer;
        }).collect(Collectors.toList());
    }

    private void setDefaultKafkaImage(String kafkaVersion) {
        DEFAULT_KAFKA_IMAGE = DockerImageName.parse("quay.io/ogunalp/kafka-native:" + kafkaVersion + "-snapshot");
        DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse("quay.io/ogunalp/zookeeper-native:" + kafkaVersion + "-snapshot");
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
        return brokers.stream()
                .map(b -> String.format("localhost:%d", b.getMappedPort(KAFKA_PORT)))
                .collect(Collectors.joining(","));
    }

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
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers(),
                user, password);
    }

    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort to for use.
    public static class KafkaContainer extends LoggingGenericContainer<KafkaContainer> {

        public KafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }

    }

    public static class ZookeeperContainer extends LoggingGenericContainer<ZookeeperContainer> {
        public ZookeeperContainer(DockerImageName zookeeperImage) {
            super(zookeeperImage);
        }
    }

    public static class LoggingGenericContainer<C extends GenericContainer<C>>
            extends GenericContainer<C> {
        private static final String CONTAINER_LOGS_DIR = "container.logs.dir";
        private String name;

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

        public LoggingGenericContainer<C> withName(String name) {
            this.name = name;
            return this;
        }
    }

}
