/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.testcontainers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.VersionComponent;
import com.github.dockerjava.api.model.Volume;

import io.kroxylicious.testing.kafka.common.KafkaClusterDriver;
import io.kroxylicious.testing.kafka.common.KafkaEndpoints;
import io.kroxylicious.testing.kafka.common.KafkaNode;
import io.kroxylicious.testing.kafka.common.KafkaNodeConfiguration;
import io.kroxylicious.testing.kafka.common.Listener;
import io.kroxylicious.testing.kafka.common.PortAllocator;
import io.kroxylicious.testing.kafka.common.Zookeeper;
import io.kroxylicious.testing.kafka.common.ZookeeperConfig;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class TestcontainersKafkaClusterDriver implements KafkaClusterDriver, KafkaEndpoints {
    private static final System.Logger LOGGER = System.getLogger(TestcontainersKafkaClusterDriver.class.getName());

    private static final int CONTAINER_STARTUP_ATTEMPTS = 3;
    private static final boolean CONTAINER_ENGINE_PODMAN = isContainerEnginePodman();
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
    public static final String WILDCARD_BIND_ADDRESS = "0.0.0.0";

    static final String KAFKA_CONTAINER_MOUNT_POINT = "/kafka";
    private static final DateTimeFormatter NAME_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    // This uid needs to match the uid used by the kafka container to execute the kafka process
    private static final String KAFKA_CONTAINER_UID = "1001";
    private final boolean kraftMode;
    private final PortAllocator portsAllocator = new PortAllocator();
    private static final String QUAY_KAFKA_IMAGE_REPO = "quay.io/ogunalp/kafka-native";
    private static final String QUAY_ZOOKEEPER_IMAGE_REPO = "quay.io/ogunalp/zookeeper-native";
    private static DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse(QUAY_KAFKA_IMAGE_REPO + ":latest-snapshot");
    private static DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse(QUAY_ZOOKEEPER_IMAGE_REPO + ":latest-snapshot");

    private static final Set<String> volumesPendingCleanup = ConcurrentHashMap.newKeySet();

    private final DockerImageName kafkaImage;
    private final DockerImageName zookeeperImage;
    private final String name;
    private final Network network = Network.newNetwork();

    private final String logDirVolumeName = createNamedVolume();
    private TestcontainersZookeeper zookeeper;

    public TestcontainersKafkaClusterDriver(TestInfo testInfo, boolean kraftMode, String kafkaVersion, DockerImageName kafkaImage, DockerImageName zookeeperImage) {
        this.kraftMode = kraftMode;
        setDefaultKafkaImage(kafkaVersion);
        this.kafkaImage = Optional.ofNullable(kafkaImage).orElse(DEFAULT_KAFKA_IMAGE);
        this.zookeeperImage = Optional.ofNullable(zookeeperImage).orElse(DEFAULT_ZOOKEEPER_IMAGE);
        this.name = Optional.ofNullable(testInfo)
                .map(TestInfo::getDisplayName)
                .map(s -> s.replaceFirst("\\(\\)$", ""))
                .map(s -> String.format("%s.%s", s, NAME_DATE_TIME_FORMAT.format(OffsetDateTime.now(Clock.systemUTC()))))
                .orElse(null);
    }

    static {
        if (!System.getenv().containsKey("TESTCONTAINERS_RYUK_DISABLED")) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "As per https://github.com/containers/podman/issues/7927#issuecomment-731525556 if using podman, set env var TESTCONTAINERS_RYUK_DISABLED=true");
        }
        // Install a shutdown hook to remove any volumes that remain. This is a best effort approach to leaving
        // container resources behind.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Set.copyOf(volumesPendingCleanup).forEach(TestcontainersKafkaClusterDriver::removeNamedVolume)));
    }

    private static DockerClient createDockerClient() {
        List<DockerClientProviderStrategy> configurationStrategies = new ArrayList<>();
        ServiceLoader.load(DockerClientProviderStrategy.class).forEach(configurationStrategies::add);
        DockerClientProviderStrategy firstValidStrategy = DockerClientProviderStrategy.getFirstValidStrategy(configurationStrategies);
        return firstValidStrategy.getDockerClient();
    }

    private static void removeNamedVolume(String name) {
        try (DockerClient dockerClient = createDockerClient()) {
            dockerClient.removeVolumeCmd(name).exec();
        }
        catch (NotFoundException ignored) {
            // volume is gone
            volumesPendingCleanup.remove(name);
        }
        catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to remove container volume {0}.", name, t);
            LOGGER.log(System.Logger.Level.WARNING, "Please run `(podman|docker) volume ls` and check for orphaned resources.");
        }
    }

    private void setDefaultKafkaImage(String kafkaVersion) {
        String kafkaVersionTag = (kafkaVersion == null || kafkaVersion.equals("latest")) ? "latest" : "latest-kafka-" + kafkaVersion;
        DEFAULT_KAFKA_IMAGE = DockerImageName.parse(QUAY_KAFKA_IMAGE_REPO + ":" + kafkaVersionTag);
        DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse(QUAY_ZOOKEEPER_IMAGE_REPO + ":" + kafkaVersionTag);
    }

    @Override
    public void nodeRemoved(KafkaNode node) {
        portsAllocator.deallocate(node.nodeId());
        try (var cleanBrokerLogDir = new OneShotContainer()) {
            cleanBrokerLogDir.withName("cleanBrokerLogDir")
                    .addGenericBind(new Bind(logDirVolumeName, new Volume(KAFKA_CONTAINER_MOUNT_POINT)))
                    .withCommand("rm", "-rf", getBrokerLogDirectory(node.nodeId()));
            cleanBrokerLogDir.start();
        }
    }

    @Override
    public EndpointPair getEndpointPair(Listener listener, int nodeId) {
        switch (listener) {
            case EXTERNAL -> {
                return buildExposedEndpoint(listener, nodeId, CLIENT_PORT);
            }
            case ANON -> {
                return buildExposedEndpoint(listener, nodeId, ANON_PORT);
            }
            case INTERNAL -> {
                return EndpointPair.builder().bind(new Endpoint(WILDCARD_BIND_ADDRESS, INTER_BROKER_PORT))
                        .connect(new Endpoint(String.format("broker-%d", nodeId), INTER_BROKER_PORT)).build();
            }
            case CONTROLLER -> {
                EndpointPair result;
                if (kraftMode) {
                    result = EndpointPair.builder().bind(new Endpoint(WILDCARD_BIND_ADDRESS, CONTROLLER_PORT))
                            .connect(new Endpoint(String.format("broker-%d", nodeId), CONTROLLER_PORT)).build();
                }
                else {
                    result = EndpointPair.builder().bind(new Endpoint(WILDCARD_BIND_ADDRESS, ZOOKEEPER_PORT)).connect(new Endpoint("zookeeper", ZOOKEEPER_PORT)).build();
                }
                return result;
            }
            default -> throw new IllegalStateException("Unexpected value: " + listener);
        }
    }

    private EndpointPair buildExposedEndpoint(Listener listener, int nodeId, int bindPort) {
        return EndpointPair.builder()
                .bind(new Endpoint(WILDCARD_BIND_ADDRESS, bindPort))
                .connect(new Endpoint("localhost", getDynamicPort(listener, nodeId)))
                .build();
    }

    private int getDynamicPort(Listener listener, int nodeId) {
        if (!portsAllocator.hasRegisteredPort(listener, nodeId)) {
            try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
                portAllocationSession.allocate(Set.of(listener), nodeId);
            }
        }
        return portsAllocator.getPort(listener, nodeId);
    }

    @Override
    public KafkaNode createNode(KafkaNodeConfiguration node) {
        ZookeeperContainer zookeeperContainer = zookeeper == null ? null : zookeeper.zookeeper;
        return new TestcontainersKafkaNode(kafkaImage, node, zookeeperContainer, name, network, logDirVolumeName);
    }

    @NotNull
    static String getBrokerLogDirectory(int brokerNum) {
        return KAFKA_CONTAINER_MOUNT_POINT + "/broker-" + brokerNum;
    }

    @Override
    public Zookeeper createZookeeper(ZookeeperConfig zookeeperConfig) {
        if (zookeeper != null) {
            throw new IllegalStateException("cannot create a second zookeeper");
        }
        zookeeper = new TestcontainersZookeeper(zookeeperImage, name, network);
        return zookeeper;
    }

    @Override
    public void close() {
        try {
            network.close();
        }
        finally {
            removeNamedVolume(this.logDirVolumeName);
        }
    }

    @SuppressWarnings({ "try" })
    private static String createNamedVolume() {
        try (DockerClient dockerClient = createDockerClient(); var volumeCmd = dockerClient.createVolumeCmd();) {
            if (CONTAINER_ENGINE_PODMAN) {
                volumeCmd.withDriverOpts(Map.of("o", "uid=" + KAFKA_CONTAINER_UID));
            }
            var volumeName = volumeCmd.exec().getName();
            volumesPendingCleanup.add(volumeName);
            if (!CONTAINER_ENGINE_PODMAN) {
                // On Docker, use a container to chown the volume.
                // This is a workaround for https://github.com/moby/moby/issues/45714
                try (var c = new OneShotContainer()) {
                    c.withName("prepareKafkaVolume")
                            .addGenericBind(new Bind(volumeName, new Volume(KAFKA_CONTAINER_MOUNT_POINT)))
                            .withCommand("chown", "-R", KAFKA_CONTAINER_UID, KAFKA_CONTAINER_MOUNT_POINT)
                            .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
                    c.start();
                }
            }
            return volumeName;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isContainerEnginePodman() {
        try (DockerClient dockerClient = createDockerClient()) {
            var ver = dockerClient.versionCmd().exec();
            var hasComponentNamedPodman = Optional.ofNullable(ver.getComponents()).stream().flatMap(Collection::stream).map(VersionComponent::getName)
                    .filter(Objects::nonNull)
                    .map(s -> s.toLowerCase(Locale.ROOT)).anyMatch(n -> n.contains("podman"));
            LOGGER.log(System.Logger.Level.INFO, "Detected container engine as Podman : {0}", hasComponentNamedPodman);
            return hasComponentNamedPodman;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getKafkaVersion() {
        return kafkaImage.getVersionPart();
    }

    /**
     * The type Kafka container.
     */
    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort for use.
    public static class KafkaContainer extends TestcontainersKafkaClusterDriver.LoggingGenericContainer<TestcontainersKafkaClusterDriver.KafkaContainer> {

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
    public static class ZookeeperContainer extends TestcontainersKafkaClusterDriver.LoggingGenericContainer<TestcontainersKafkaClusterDriver.ZookeeperContainer> {
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
     * Container used for running one-short commands.
     */
    public static class OneShotContainer extends TestcontainersKafkaClusterDriver.LoggingGenericContainer<TestcontainersKafkaClusterDriver.OneShotContainer> {
        /**
         * Instantiates a new one-shot container
         */
        public OneShotContainer() {
            super(DockerImageName.parse("registry.access.redhat.com/ubi9/ubi-minimal"));
            this.withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS);
            this.withStartupCheckStrategy(new OneShotStartupCheckStrategy());
        }
    }

    /**
     * The type Logging generic container.
     *
     * @param <C> the type parameter
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
                try (var writer = new FileWriter(target.toFile())) {
                    LOGGER.log(System.Logger.Level.DEBUG, "writing logs for {0} to {1}", getContainerName(), target);
                    super.followOutput(outputFrame -> {
                        try {
                            if (outputFrame.equals(OutputFrame.END)) {
                                writer.close();
                            }
                            else {
                                writer.write(outputFrame.getUtf8String());
                                writer.flush();
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
        public TestcontainersKafkaClusterDriver.LoggingGenericContainer<C> withName(String name) {
            this.name = name;
            return this;
        }

        public TestcontainersKafkaClusterDriver.LoggingGenericContainer<C> addGenericBind(Bind bind) {
            super.getBinds().add(bind);
            return this;
        }

    }

}
