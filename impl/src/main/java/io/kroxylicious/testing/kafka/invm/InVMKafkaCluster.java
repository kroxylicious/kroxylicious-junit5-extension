/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.jetbrains.annotations.NotNull;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.KafkaServer;
import kafka.server.Server;
import kafka.tools.StorageTool;
import scala.Option;
import scala.collection.immutable.Seq;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.ListeningSocketPreallocator;
import io.kroxylicious.testing.kafka.common.Utils;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaCluster implements KafkaCluster {
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaCluster.class.getName());
    private static final int STARTUP_TIMEOUT = 30;

    private final KafkaClusterConfig clusterConfig;
    private final Path tempDirectory;
    private ZooKeeperServer zooServer;

    /**
     * Map of kafka <code>node.id</code> to {@link Server}.
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final Map<Integer, Server> servers = new HashMap<>();

    /**
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private KafkaClusterConfig.KafkaEndpoints kafkaEndpoints;
    /**
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final SortedMap<Integer, ServerSocket> externalPorts = new TreeMap<>();
    /**
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final SortedMap<Integer, ServerSocket> anonPorts = new TreeMap<>();
    /**
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final SortedMap<Integer, ServerSocket> interBrokerPorts = new TreeMap<>();
    /**
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final SortedMap<Integer, ServerSocket> controllerPorts = new TreeMap<>();

    /**
     * Instantiates a new in VM kafka cluster.
     *
     * @param clusterConfig the cluster config
     */
    public InVMKafkaCluster(KafkaClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        try {
            tempDirectory = Files.createTempDirectory("kafka");
            tempDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<ServerSocket> allocateControllerPorts(ListeningSocketPreallocator preallocator, KafkaClusterConfig clusterConfig) {
        if (clusterConfig.isKraftMode()) {
            return preallocator.preAllocateListeningSockets(clusterConfig.getKraftControllers());
        }
        else {
            return preallocator.preAllocateListeningSockets(1);
        }
    }

    @NotNull
    private Server buildKafkaServer(KafkaClusterConfig.ConfigHolder c) {
        KafkaConfig config = buildBrokerConfig(c, tempDirectory);
        Option<String> threadNamePrefix = Option.apply(null);

        boolean kraftMode = clusterConfig.isKraftMode();
        if (kraftMode) {
            var clusterId = c.getKafkaKraftClusterId();
            var directories = StorageTool.configToLogDirectories(config);
            ensureDirectoriesAreEmpty(directories);
            var metaProperties = StorageTool.buildMetadataProperties(clusterId, config);
            StorageTool.formatCommand(System.out, directories, metaProperties, MINIMUM_BOOTSTRAP_VERSION, true);
            return new KafkaRaftServer(config, Time.SYSTEM, threadNamePrefix);
        }
        else {
            return new KafkaServer(config, Time.SYSTEM, threadNamePrefix, false);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void ensureDirectoriesAreEmpty(Seq<String> directories) {
        directories.foreach(pathStr -> {
            final Path path = Path.of(pathStr);
            if (Files.exists(path)) {
                try (var ps = Files.walk(path);
                        var s = ps
                                .sorted(Comparator.reverseOrder())
                                .map(Path::toFile)) {
                    s.forEach(File::delete);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return true;
        });
    }

    @NotNull
    private KafkaConfig buildBrokerConfig(KafkaClusterConfig.ConfigHolder c, Path tempDirectory) {
        Properties properties = new Properties();
        properties.putAll(c.getProperties());
        Path logsDir = tempDirectory.resolve(String.format("broker-%d", c.getBrokerNum()));
        properties.setProperty(KafkaConfig.LogDirProp(), logsDir.toAbsolutePath().toString());
        LOGGER.log(System.Logger.Level.DEBUG, "Generated config {0}", properties);
        return new KafkaConfig(properties);
    }

    @Override
    public synchronized void start() {
        // kraft mode: per-broker: 1 external port + 1 inter-broker port + 1 controller port + 1 anon port
        // zk mode: per-cluster: 1 zk port; per-broker: 1 external port + 1 inter-broker port + 1 anon port
        try (var preallocator = new ListeningSocketPreallocator()) {
            Utils.putAllListEntriesIntoMapKeyedByIndex(preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()), externalPorts);
            Utils.putAllListEntriesIntoMapKeyedByIndex(preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()), anonPorts);
            Utils.putAllListEntriesIntoMapKeyedByIndex(preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()), interBrokerPorts);
            Utils.putAllListEntriesIntoMapKeyedByIndex(allocateControllerPorts(preallocator, clusterConfig), controllerPorts);
        }

        kafkaEndpoints = new KafkaClusterConfig.KafkaEndpoints() {

            @Override
            public EndpointPair getClientEndpoint(int brokerId) {
                return buildEndpointPair(externalPorts, brokerId);
            }

            @Override
            public EndpointPair getAnonEndpoint(int brokerId) {
                return buildEndpointPair(anonPorts, brokerId);
            }

            @Override
            public EndpointPair getInterBrokerEndpoint(int brokerId) {
                return buildEndpointPair(interBrokerPorts, brokerId);
            }

            @Override
            public EndpointPair getControllerEndpoint(int brokerId) {
                return buildEndpointPair(controllerPorts, brokerId);
            }

            private EndpointPair buildEndpointPair(SortedMap<Integer, ServerSocket> portRange, int brokerId) {
                var port = portRange.get(brokerId);
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port.getLocalPort())).connect(new Endpoint("localhost", port.getLocalPort())).build();
            }
        };

        buildAndStartZookeeper();
        clusterConfig.getBrokerConfigs(() -> kafkaEndpoints).parallel().forEach(configHolder -> {
            final Server server = this.buildKafkaServer(configHolder);
            tryToStartServerWithRetry(configHolder, server);
            servers.put(configHolder.getBrokerNum(), server);

        });
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getConnectConfigForCluster(buildServerList(kafkaEndpoints::getAnonEndpoint), null, null, null, null), 120, TimeUnit.SECONDS,
                clusterConfig.getBrokersNum());
    }

    private void tryToStartServerWithRetry(KafkaClusterConfig.ConfigHolder configHolder, Server server) {
        Utils.awaitCondition(STARTUP_TIMEOUT, TimeUnit.SECONDS)
                .until(() -> {
                    // Hopefully we can remove this once a fix for https://issues.apache.org/jira/browse/KAFKA-14908 actually lands.
                    try {
                        server.startup();
                        return true;
                    }
                    catch (Throwable t) {
                        LOGGER.log(System.Logger.Level.WARNING, "failed to start server due to: " + t.getMessage());
                        LOGGER.log(System.Logger.Level.WARNING, "anon: {0}, client: {1}, controller: {2}, interBroker: {3}, ",
                                kafkaEndpoints.getAnonEndpoint(configHolder.getBrokerNum()).getBind(),
                                kafkaEndpoints.getClientEndpoint(configHolder.getBrokerNum()).getBind(),
                                kafkaEndpoints.getControllerEndpoint(configHolder.getBrokerNum()).getBind(),
                                kafkaEndpoints.getInterBrokerEndpoint(configHolder.getBrokerNum()).getBind());

                        server.shutdown();
                        server.awaitShutdown();
                        return false;
                    }
                });
    }

    private void buildAndStartZookeeper() {
        if (!clusterConfig.isKraftMode()) {
            try {
                final int zookeeperPort = controllerPorts.get(0).getLocalPort();
                ServerCnxnFactory zooFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", zookeeperPort), 1024);

                var zoo = tempDirectory.resolve("zoo");
                var snapshotDir = zoo.resolve("snapshot");
                var logDir = zoo.resolve("log");
                Files.createDirectories(snapshotDir);
                Files.createDirectories(logDir);

                zooServer = new ZooKeeperServer(snapshotDir.toFile(), logDir.toFile(), 500);
                zooFactory.startup(zooServer);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getClusterId() {
        return clusterConfig.clusterId();
    }

    @Override
    public synchronized String getBootstrapServers() {
        return buildServerList(kafkaEndpoints::getClientEndpoint);
    }

    private synchronized String buildServerList(Function<Integer, KafkaClusterConfig.KafkaEndpoints.EndpointPair> endpointFunc) {
        return servers.keySet().stream()
                .map(endpointFunc)
                .map(KafkaClusterConfig.KafkaEndpoints.EndpointPair::connectAddress)
                .collect(Collectors.joining(","));
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
    public synchronized int addBroker() {
        // find next free kafka node.id
        var first = IntStream.rangeClosed(0, getNumOfBrokers()).filter(cand -> !servers.containsKey(cand)).findFirst();
        if (first.isEmpty()) {
            throw new IllegalStateException("Could not determine new nodeId, existing set " + servers.keySet());
        }
        var newNodeId = first.getAsInt();

        LOGGER.log(System.Logger.Level.DEBUG,
                "Adding broker with node.id {0} to cluster with existing nodes {1}.", newNodeId, servers.keySet());

        // preallocate ports for the new broker
        try (var preallocator = new ListeningSocketPreallocator()) {
            externalPorts.put(newNodeId, preallocator.preAllocateListeningSockets(1).get(0));
            anonPorts.put(newNodeId, preallocator.preAllocateListeningSockets(1).get(0));
            interBrokerPorts.put(newNodeId, preallocator.preAllocateListeningSockets(1).get(0));
        }

        var configHolder = clusterConfig.generateConfigForSpecificBroker(kafkaEndpoints, newNodeId);
        final Server server = buildKafkaServer(configHolder);
        tryToStartServerWithRetry(configHolder, server);
        servers.put(configHolder.getBrokerNum(), server);

        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getConnectConfigForCluster(buildServerList(kafkaEndpoints::getAnonEndpoint), null, null, null, null), 120, TimeUnit.SECONDS,
                getNumOfBrokers());
        return configHolder.getBrokerNum();
    }

    @Override
    public synchronized void removeBroker(int nodeId) throws IllegalArgumentException, UnsupportedOperationException {
        if (!servers.containsKey(nodeId)) {
            throw new IllegalArgumentException("Broker node " + nodeId + " is not a member of the cluster.");
        }
        if (clusterConfig.isKraftMode() && controllerPorts.containsKey(nodeId)) {
            throw new UnsupportedOperationException("Cannot remove controller node " + nodeId + " from a kraft cluster.");
        }
        if (servers.size() < 2) {
            throw new IllegalArgumentException("Cannot remove a node from a cluster with only %d nodes".formatted(servers.size()));
        }

        var target = servers.keySet().stream().filter(n -> n != nodeId).findFirst();
        if (target.isEmpty()) {
            throw new IllegalStateException("Could not identify a node to be the re-assignment target");
        }

        Utils.awaitReassignmentOfKafkaInternalTopicsIfNecessary(
                clusterConfig.getConnectConfigForCluster(buildServerList(kafkaEndpoints::getAnonEndpoint), null, null, null, null), nodeId,
                target.get(), 120, TimeUnit.SECONDS);

        externalPorts.remove(nodeId);
        anonPorts.remove(nodeId);
        interBrokerPorts.remove(nodeId);

        var s = servers.remove(nodeId);
        s.shutdown();
        s.awaitShutdown();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public synchronized void close() throws Exception {
        try {
            try {
                // shutdown controller last seems to be significant to the kraft case.
                servers.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                        .map(Map.Entry::getValue)
                        .forEach(Server::shutdown);
            }
            finally {
                if (zooServer != null) {
                    zooServer.shutdown(true);
                }
            }
        }
        finally {
            if (tempDirectory.toFile().exists()) {
                try (var ps = Files.walk(tempDirectory);
                        var s = ps
                                .sorted(Comparator.reverseOrder())
                                .map(Path::toFile)) {
                    s.forEach(File::delete);
                }
            }
        }
    }

    @Override
    public synchronized int getNumOfBrokers() {
        return servers.size();
    }
}
