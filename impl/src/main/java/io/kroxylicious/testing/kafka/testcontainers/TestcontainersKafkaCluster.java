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
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.config.SslConfigs;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.VersionComponent;
import com.github.dockerjava.api.model.Volume;

import kafka.server.KafkaConfig;
import lombok.SneakyThrows;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.PortAllocator;
import io.kroxylicious.testing.kafka.common.Utils;

import static io.kroxylicious.testing.kafka.common.Utils.awaitExpectedBrokerCountInClusterViaTopic;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class TestcontainersKafkaCluster implements Startable, KafkaCluster, KafkaClusterConfig.KafkaEndpoints {

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
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration RESTART_BACKOFF_DELAY = Duration.ofMillis(2500);

    // If Zookeeper or Kafka run for less than 500ms, there's almost certainly a problem. This makes it be treated
    // as a startup failure.
    private static final Duration MINIMUM_RUNNING_DURATION = Duration.ofMillis(500);
    private static final boolean CONTAINER_ENGINE_PODMAN = isContainerEnginePodman();
    private static final String KAFKA_CONTAINER_MOUNT_POINT = "/kafka";
    private static DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse(QUAY_KAFKA_IMAGE_REPO + ":latest-snapshot");
    private static DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse(QUAY_ZOOKEEPER_IMAGE_REPO + ":latest-snapshot");

    // This uid needs to match the uid used by the kafka container to execute the kafka process
    private static final String KAFKA_CONTAINER_UID = "1001";
    private static final int READY_TIMEOUT_SECONDS = 120;
    private final DockerImageName kafkaImage;
    private final DockerImageName zookeeperImage;
    private final KafkaClusterConfig clusterConfig;
    private final String logDirVolumeName = createNamedVolume();
    private final Network network = Network.newNetwork();
    private final String name;
    private final ZookeeperContainer zookeeper;

    /**
     * Map of kafka <code>node.id</code> to {@link KafkaContainer}.
     * Protected by lock of {@link TestcontainersKafkaCluster itself.}
     */
    private final Map<Integer, KafkaContainer> brokers = new TreeMap<>();

    /**
     * Set of kafka <code>node.id</code> that are currently stopped.
     * Protected by lock of {@link TestcontainersKafkaCluster itself.}
     */
    private final Set<Integer> stoppedBrokers = new HashSet<>();

    private final PortAllocator portsAllocator = new PortAllocator();
    private static final Set<String> volumesPendingCleanup = ConcurrentHashMap.newKeySet();

    static {
        if (!System.getenv().containsKey("TESTCONTAINERS_RYUK_DISABLED")) {
            LOGGER.log(Level.WARNING,
                    "As per https://github.com/containers/podman/issues/7927#issuecomment-731525556 if using podman, set env var TESTCONTAINERS_RYUK_DISABLED=true");
        }
        // Install a shutdown hook to remove any volumes that remain. This is a best effort approach to leaving
        // container resources behind.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Set.copyOf(volumesPendingCleanup).forEach(TestcontainersKafkaCluster::removeNamedVolume)));
    }

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

        this.name = Optional.ofNullable(clusterConfig.getTestInfo())
                .map(TestInfo::getDisplayName)
                .map(s -> s.replaceFirst("\\(\\)$", ""))
                .map(s -> String.format("%s.%s", s, OffsetDateTime.now(Clock.systemUTC())))
                .orElse(null);

        if (this.clusterConfig.isKraftMode()) {
            this.zookeeper = null;
        }
        else {
            this.zookeeper = new ZookeeperContainer(this.zookeeperImage)
                    .withName(name)
                    .withNetwork(network)
                    .withMinimumRunningDuration(MINIMUM_RUNNING_DURATION)
                    .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.zookeeper logging too
                    .withNetworkAliases("zookeeper");
        }

        try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
            portAllocationSession.allocate(Set.of(Listener.EXTERNAL, Listener.ANON), 0, clusterConfig.getBrokersNum());
        }

        clusterConfig.getBrokerConfigs(() -> this).forEach(holder -> brokers.put(holder.getBrokerNum(), buildKafkaContainer(holder)));
    }

    @NotNull
    private KafkaContainer buildKafkaContainer(KafkaClusterConfig.ConfigHolder holder) {
        String netAlias = "broker-" + holder.getBrokerNum();
        KafkaContainer kafkaContainer = new KafkaContainer(kafkaImage)
                .withName(name)
                .withNetwork(network)
                .withNetworkAliases(netAlias);

        copyHostKeyStoreToContainer(kafkaContainer, holder.getProperties(), SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        copyHostKeyStoreToContainer(kafkaContainer, holder.getProperties(), SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);

        kafkaContainer
                // KAFKA_LOG_DIR overrides a key in the quarkus kafka image application.properties. The quarkus app uses
                // that to set log.dir. Any value we set for log.dir in server.properties is lost.
                .withEnv("KAFKA_LOG_DIR", getBrokerLogDirectory(holder.getBrokerNum()))
                // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.kafka logging too
                .withEnv("SERVER_PROPERTIES_FILE", "/cnf/server.properties")
                .withEnv("SERVER_CLUSTER_ID", holder.getKafkaKraftClusterId())
                .withCopyToContainer(Transferable.of(propertiesToBytes(holder.getProperties()), 0644), "/cnf/server.properties")
                .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                .withMinimumRunningDuration(MINIMUM_RUNNING_DURATION)
                .withStartupTimeout(STARTUP_TIMEOUT);
        kafkaContainer.addFixedExposedPort(holder.getExternalPort(), CLIENT_PORT);
        kafkaContainer.addFixedExposedPort(holder.getAnonPort(), ANON_PORT);
        kafkaContainer.addGenericBind(new Bind(logDirVolumeName, new Volume(KAFKA_CONTAINER_MOUNT_POINT)));

        if (!clusterConfig.isKraftMode()) {
            kafkaContainer.dependsOn(this.zookeeper);
        }
        return kafkaContainer;
    }

    @NotNull
    private String getBrokerLogDirectory(int brokerNum) {
        return KAFKA_CONTAINER_MOUNT_POINT + "/broker-" + brokerNum;
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
    public synchronized String getBootstrapServers() {
        return buildServerList(nodeId -> this.getEndpointPair(Listener.EXTERNAL, nodeId));
    }

    private synchronized String buildServerList(Function<Integer, KafkaClusterConfig.KafkaEndpoints.EndpointPair> endpointFunc) {
        return brokers.keySet().stream()
                .map(endpointFunc)
                .map(KafkaClusterConfig.KafkaEndpoints.EndpointPair::connectAddress)
                .collect(Collectors.joining(","));
    }

    /**
     * Gets kafka version.
     *
     * @return the kafka version
     */
    public String getKafkaVersion() {
        return kafkaImage.getVersionPart();
    }

    private synchronized Stream<GenericContainer<?>> allContainers() {
        return Stream.concat(
                this.brokers.values().stream(),
                Stream.ofNullable(this.zookeeper));
    }

    @Override
    @SneakyThrows
    public synchronized void start() {
        try {
            if (zookeeper != null) {
                zookeeper.start();
            }
            Startables.deepStart(brokers.values().stream()).get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            awaitExpectedBrokerCountInClusterViaTopic(
                    clusterConfig.getAnonConnectConfigForCluster(buildServerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))),
                    READY_TIMEOUT_SECONDS, TimeUnit.SECONDS,
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
    public synchronized int addBroker() {
        // find next free kafka node.id
        var first = IntStream.rangeClosed(0, getNumOfBrokers()).filter(cand -> !brokers.containsKey(cand)).findFirst();
        if (first.isEmpty()) {
            throw new IllegalStateException("Could not determine new nodeId, existing set " + brokers.keySet());
        }
        var newNodeId = first.getAsInt();

        LOGGER.log(System.Logger.Level.DEBUG,
                "Adding broker with node.id {0} to cluster with existing nodes {1}.", newNodeId, brokers.keySet());

        // preallocate ports for the new broker
        try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
            portAllocationSession.allocate(Set.of(Listener.EXTERNAL, Listener.ANON), newNodeId);
        }

        var configHolder = clusterConfig.generateConfigForSpecificNode(this, newNodeId);
        var kafkaContainer = buildKafkaContainer(configHolder);
        try {
            Startables.deepStart(kafkaContainer).get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            kafkaContainer.stop();
            throw new RuntimeException(e);
        }
        brokers.put(configHolder.getBrokerNum(), kafkaContainer);

        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getAnonConnectConfigForCluster(buildServerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
        return configHolder.getBrokerNum();
    }

    @Override
    public synchronized void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException {
        if (!brokers.containsKey(nodeId)) {
            throw new IllegalArgumentException("Broker node " + nodeId + " is not a member of the cluster.");
        }
        if (clusterConfig.isKraftMode() && nodeId < clusterConfig.getKraftControllers()) {
            throw new UnsupportedOperationException("Cannot remove controller node " + nodeId + " from a kraft cluster.");
        }
        if (brokers.size() < 2) {
            throw new IllegalArgumentException("Cannot remove a node from a cluster with only %d nodes".formatted(brokers.size()));
        }
        if (!stoppedBrokers.isEmpty()) {
            throw new IllegalStateException("Cannot remove nodes from a cluster with stopped nodes.");
        }

        var target = brokers.keySet().stream().filter(n -> n != nodeId).findFirst();
        if (target.isEmpty()) {
            throw new IllegalStateException("Could not identify a node to be the re-assignment target");
        }

        Utils.awaitReassignmentOfKafkaInternalTopicsIfNecessary(
                clusterConfig.getAnonConnectConfigForCluster(buildServerList(id -> getEndpointPair(Listener.ANON, id))), nodeId,
                target.get(), 120, TimeUnit.SECONDS);

        portsAllocator.deallocate(nodeId);

        gracefulStop(brokers.remove(nodeId));

        try (var cleanBrokerLogDir = new OneShotContainer()) {
            cleanBrokerLogDir.withName("cleanBrokerLogDir")
                    .addGenericBind(new Bind(logDirVolumeName, new Volume(KAFKA_CONTAINER_MOUNT_POINT)))
                    .withCommand("rm", "-rf", getBrokerLogDirectory(nodeId));
            cleanBrokerLogDir.start();
        }
    }

    private void gracefulStop(KafkaContainer kafkaContainer) {
        // https://github.com/testcontainers/testcontainers-java/issues/1000
        // Note that GenericContainer#stop actually implements stop using kill, so the broker doesn't have chance to
        // tell the controller that it is going away.

        var containerId = kafkaContainer.getContainerId();
        try (var stopContainerCmd = kafkaContainer.getDockerClient().stopContainerCmd(containerId);
                var waitCmd = kafkaContainer.getDockerClient().waitContainerCmd(containerId)) {
            stopContainerCmd.exec();
            var statusCode = waitCmd.start().awaitStatusCode(10, TimeUnit.SECONDS);
            LOGGER.log(Level.DEBUG, "Shut-down broker {0}, exit status {1}", containerId, statusCode);
        }
        catch (Exception e) {
            LOGGER.log(Level.WARNING, "Ignoring exception whilst shutting down broker {0}", containerId, e);
        }
        finally {
            // We need to do this regardless so that Testcontainer's internal state is correct.
            kafkaContainer.stop();
        }
    }

    @Override
    public synchronized void stopNodes(Predicate<Integer> nodeIdPredicate, TerminationStyle terminationStyle) {
        var kafkaContainersToStop = brokers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> !stoppedBrokers.contains(e.getKey()))
                .toList();

        if (kafkaContainersToStop.isEmpty()) {
            return;
        }

        LOGGER.log(Level.DEBUG, "Stopping {0}/{1} nodes(s)", kafkaContainersToStop.size(), getNumOfBrokers());

        var inspectCommands = kafkaContainersToStop.stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> buildInspectionCommandFor(e.getValue())));

        kafkaContainersToStop.stream().map(Map.Entry::getValue).forEach(kc -> {
            if (terminationStyle == TerminationStyle.ABRUPT) {
                kc.stop();
            }
            else {
                gracefulStop(kc);
            }
        });

        // Await the existing containers going away.
        inspectCommands
                .forEach((nodeId, inspectContainer) -> Utils.awaitCondition(Long.valueOf(STARTUP_TIMEOUT.toMillis()).intValue(), TimeUnit.MILLISECONDS).until(() -> {
                    try {
                        inspectContainer.exec();
                    }
                    catch (NotFoundException e) {
                        stoppedBrokers.add(nodeId);
                        return true;
                    }
                    return false;
                }));

        if (zookeeper != null) {
            // In the zookeeper case, we may as well wait for the zookeeper session timeout. This serves to prevent a
            // subsequent call to startBroker spinning excessively.
            var config = clusterConfig.getBrokerConfigs(() -> this).findFirst();
            var zkSessionTimeout = config.map(KafkaClusterConfig.ConfigHolder::getProperties).map(p -> p.getProperty(KafkaConfig.ZkSessionTimeoutMsProp(), "0"))
                    .map(Long::parseLong);
            zkSessionTimeout.filter(timeout -> timeout > 0).ifPresent(
                    timeOut -> {
                        try {
                            LOGGER.log(Level.DEBUG, "Awaiting zookeeper session timeout {0}ms so that the broker ephemeral nodes expire.", timeOut);
                            Thread.sleep(timeOut);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    @Override
    public synchronized void startNodes(Predicate<Integer> nodeIdPredicate) {
        var kafkaContainersToStart = brokers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> stoppedBrokers.contains(e.getKey()))
                .toList();

        if (kafkaContainersToStart.isEmpty()) {
            return;
        }

        LOGGER.log(Level.DEBUG, "Starting {0}/{1} node(s)", kafkaContainersToStart.size(), getNumOfBrokers());
        kafkaContainersToStart.forEach(entry -> {
            var nodeId = entry.getKey();
            var kc = entry.getValue();
            var originalStartupAttempts = kc.getStartupAttempts();
            try {
                // We need to control the restart loop ourselves. Unfortunately testcontainers does not
                // allow a delay to be introduced between startup attempt. Start-ups in a tight loop
                // seems to lead to podman unreliability (at least on the Mac).
                kc.withStartupAttempts(1);
                Awaitility.waitAtMost(STARTUP_TIMEOUT)
                        .pollDelay(RESTART_BACKOFF_DELAY)
                        .until(() -> {
                            try {
                                kc.start();
                                return true;
                            }
                            catch (Exception e) {
                                kc.stop();
                                LOGGER.log(Level.DEBUG, "Failed to restart container", e);
                                return false;
                            }
                        });

                ;
            }
            finally {
                kc.setStartupAttempts(originalStartupAttempts);
                stoppedBrokers.remove(nodeId);
            }
        });
    }

    @Override
    public void close() {
        this.stop();
    }

    @Override
    public synchronized int getNumOfBrokers() {
        return brokers.size();
    }

    @Override
    public synchronized Set<Integer> getStoppedBrokers() {
        return Set.copyOf(this.stoppedBrokers);
    }

    @Override
    public synchronized void stop() {
        try {
            allContainers().parallel().forEach(GenericContainer::stop);
        }
        finally {
            try {
                network.close();
            }
            finally {
                removeNamedVolume(this.logDirVolumeName);
            }
        }
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

    @Override
    public synchronized EndpointPair getEndpointPair(Listener listener, int nodeId) {
        switch (listener) {
            case EXTERNAL -> {
                return buildExposedEndpoint(listener, nodeId, CLIENT_PORT);
            }
            case ANON -> {
                return buildExposedEndpoint(listener, nodeId, ANON_PORT);
            }
            case INTERNAL -> {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", INTER_BROKER_PORT))
                        .connect(new Endpoint(String.format("broker-%d", nodeId), INTER_BROKER_PORT)).build();
            }
            case CONTROLLER -> {
                EndpointPair result;
                if (clusterConfig.isKraftMode()) {
                    result = EndpointPair.builder().bind(new Endpoint("0.0.0.0", CONTROLLER_PORT))
                            .connect(new Endpoint(String.format("broker-%d", nodeId), CONTROLLER_PORT)).build();
                }
                else {
                    result = EndpointPair.builder().bind(new Endpoint("0.0.0.0", ZOOKEEPER_PORT)).connect(new Endpoint("zookeeper", ZOOKEEPER_PORT)).build();
                }
                return result;
            }
            default -> throw new IllegalStateException("Unexpected value: " + listener);
        }
    }

    private EndpointPair buildExposedEndpoint(Listener listener, int nodeId, int bindPort) {
        return EndpointPair.builder()
                .bind(new Endpoint("0.0.0.0", bindPort))
                .connect(new Endpoint("localhost", portsAllocator.getPort(listener, nodeId)))
                .build();
    }

    private InspectContainerCmd buildInspectionCommandFor(KafkaContainer kc) {
        return kc.getDockerClient().inspectContainerCmd(kc.getContainerId());
    }

    @SuppressWarnings({ "try" })
    private static String createNamedVolume() {
        var volumeCmd = DockerClientFactory.lazyClient().createVolumeCmd();
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

    @SuppressWarnings({ "try" })
    private static void removeNamedVolume(String name) {
        try {
            DockerClientFactory.lazyClient().removeVolumeCmd(name).exec();
        }
        catch (NotFoundException ignored) {
            // volume is gone
            volumesPendingCleanup.remove(name);
        }
        catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Failed to remove container volume {0}.", name, t);
            LOGGER.log(Level.WARNING, "Please run `(podman|docker) volume ls` and check for orphaned resources.");
        }
    }

    private static boolean isContainerEnginePodman() {
        var ver = DockerClientFactory.lazyClient().versionCmd().exec();
        var hasComponentNamedPodman = Optional.ofNullable(ver.getComponents()).stream().flatMap(Collection::stream).map(VersionComponent::getName)
                .filter(Objects::nonNull)
                .map(s -> s.toLowerCase(Locale.ROOT)).anyMatch(n -> n.contains("podman"));
        LOGGER.log(Level.INFO, "Detected container engine as Podman : {0}", hasComponentNamedPodman);
        return hasComponentNamedPodman;
    }

    /**
     * The type Kafka container.
     */
    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort for use.
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
     * Container used for running one-short commands.
     */
    public static class OneShotContainer extends LoggingGenericContainer<OneShotContainer> {
        /**
         * Instantiates a new one-shot container
         */
        public OneShotContainer() {
            super(DockerImageName.parse("busybox"));
            this.withStartupCheckStrategy(new OneShotStartupCheckStrategy());
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

        public LoggingGenericContainer<C> addGenericBind(Bind bind) {
            super.getBinds().add(bind);
            return this;
        }

    }
}
