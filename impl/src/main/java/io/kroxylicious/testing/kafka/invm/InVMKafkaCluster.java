/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.utils.Exit;
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

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.PortAllocator;
import io.kroxylicious.testing.kafka.common.Utils;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaCluster implements KafkaCluster, KafkaClusterConfig.KafkaEndpoints {
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaCluster.class.getName());
    private static final PrintStream LOGGING_PRINT_STREAM = LoggingPrintStream.loggingPrintStream(LOGGER, System.Logger.Level.DEBUG);
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
     * Set of kafka <code>node.id</code> that are currently stopped.
     * Protected by lock of {@link InVMKafkaCluster itself.}
     */
    private final Set<Integer> stoppedServers = new HashSet<>();

    private final PortAllocator portsAllocator = new PortAllocator();

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
        trapKafkaSystemExit();
    }

    @NotNull
    private Server buildKafkaServer(KafkaClusterConfig.ConfigHolder c) {
        KafkaConfig config = buildBrokerConfig(c);
        Option<String> threadNamePrefix = Option.apply(null);

        boolean kraftMode = clusterConfig.isKraftMode();
        if (kraftMode) {
            var clusterId = c.getKafkaKraftClusterId();
            var directories = StorageTool.configToLogDirectories(config);
            var metaProperties = StorageTool.buildMetadataProperties(clusterId, config);
            // note ignoreFormatter=true so tolerate a log directory which is already formatted. this is
            // required to support start/stop.
            StorageTool.formatCommand(LOGGING_PRINT_STREAM, directories, metaProperties, MINIMUM_BOOTSTRAP_VERSION, true);
            return instantiateKraftServer(config, threadNamePrefix);
        }
        else {
            return new KafkaServer(config, Time.SYSTEM, threadNamePrefix, false);
        }
    }

    /**
     * We instantiate the KafkaRaftServer reflectively to support running against versions of kafka
     * older than 3.5.0. This is to enable users to downgrade the broker used for embedded testing rather
     * than forcing them to increase their kafka-clients version to 3.5.0 (broker 3.5.0 requires kafka-clients 3.5.0)
     */
    @NotNull
    private Server instantiateKraftServer(KafkaConfig config, Option<String> threadNamePrefix) {
        Object kraftServer = construct(KafkaRaftServer.class, config, Time.SYSTEM)
                .orElseGet(() -> construct(KafkaRaftServer.class, config, Time.SYSTEM, threadNamePrefix).orElseThrow());
        return (Server) kraftServer;
    }

    public Optional<Object> construct(Class<?> clazz, Object... parameters) {
        Constructor<?>[] declaredConstructors = clazz.getDeclaredConstructors();
        return Arrays.stream(declaredConstructors)
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .filter(constructor -> {
                    if (constructor.getParameterCount() != parameters.length) {
                        return false;
                    }
                    boolean allMatch = true;
                    Class<?>[] parameterTypes = constructor.getParameterTypes();
                    for (int i = 0; i < parameters.length; i++) {
                        allMatch = allMatch && parameterTypes[i].isInstance(parameters[i]);
                    }
                    return allMatch;
                }).findFirst().map(constructor -> {
                    try {
                        return constructor.newInstance(parameters);
                    }
                    catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @NotNull
    private KafkaConfig buildBrokerConfig(KafkaClusterConfig.ConfigHolder c) {
        Properties properties = new Properties();
        properties.putAll(c.getProperties());
        var logsDir = getBrokerLogDir(c.getBrokerNum());
        properties.setProperty(KafkaConfig.LogDirProp(), logsDir.toAbsolutePath().toString());
        LOGGER.log(System.Logger.Level.DEBUG, "Generated config {0}", properties);
        return new KafkaConfig(properties);
    }

    @NotNull
    private Path getBrokerLogDir(int brokerNum) {
        return this.tempDirectory.resolve(String.format("broker-%d", brokerNum));
    }

    @Override
    public synchronized void start() {
        // kraft mode: per-broker: 1 external port + 1 inter-broker port + 1 controller port + 1 anon port
        // zk mode: per-cluster: 1 zk port; per-broker: 1 external port + 1 inter-broker port + 1 anon port
        try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
            portAllocationSession.allocate(Set.of(Listener.EXTERNAL, Listener.ANON, Listener.INTERNAL), 0, clusterConfig.getBrokersNum());
            portAllocationSession.allocate(Set.of(Listener.CONTROLLER), 0, clusterConfig.isKraftMode() ? clusterConfig.getKraftControllers() : 1);
        }

        buildAndStartZookeeper();
        clusterConfig.getBrokerConfigs(() -> this).parallel().forEach(configHolder -> {
            final Server server = this.buildKafkaServer(configHolder);
            tryToStartServerWithRetry(configHolder, server);
            servers.put(configHolder.getBrokerNum(), server);

        });
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getAnonConnectConfigForCluster(buildServerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))), 120,
                TimeUnit.SECONDS,
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
                                this.getEndpointPair(Listener.ANON, configHolder.getBrokerNum()).getBind(),
                                this.getEndpointPair(Listener.EXTERNAL, configHolder.getBrokerNum()).getBind(),
                                this.getEndpointPair(Listener.CONTROLLER, configHolder.getBrokerNum()).getBind(),
                                this.getEndpointPair(Listener.EXTERNAL, configHolder.getBrokerNum()).getBind());

                        server.shutdown();
                        server.awaitShutdown();
                        return false;
                    }
                });
    }

    private void buildAndStartZookeeper() {
        if (!clusterConfig.isKraftMode()) {
            try {
                final int zookeeperPort = portsAllocator.getPort(Listener.CONTROLLER, 0);
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
        return buildServerList(nodeId -> getEndpointPair(Listener.EXTERNAL, nodeId));
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
        try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
            portAllocationSession.allocate(Set.of(Listener.EXTERNAL, Listener.ANON, Listener.INTERNAL), newNodeId);
        }

        var configHolder = clusterConfig.generateConfigForSpecificNode(this, newNodeId);
        final Server server = buildKafkaServer(configHolder);
        tryToStartServerWithRetry(configHolder, server);
        servers.put(newNodeId, server);

        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getAnonConnectConfigForCluster(buildServerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
        return configHolder.getBrokerNum();
    }

    @Override
    public synchronized void removeBroker(int nodeId) throws IllegalArgumentException, UnsupportedOperationException {
        if (!servers.containsKey(nodeId)) {
            throw new IllegalArgumentException("Broker node " + nodeId + " is not a member of the cluster.");
        }
        if (clusterConfig.isKraftMode() && portsAllocator.hasRegisteredPort(Listener.CONTROLLER, nodeId)) {
            throw new UnsupportedOperationException("Cannot remove controller node " + nodeId + " from a kraft cluster.");
        }
        if (servers.size() < 2) {
            throw new IllegalArgumentException("Cannot remove a node from a cluster with only %d nodes".formatted(servers.size()));
        }
        if (!stoppedServers.isEmpty()) {
            throw new IllegalStateException("Cannot remove nodes from a cluster with stopped nodes.");
        }

        var target = servers.keySet().stream().filter(n -> n != nodeId).findFirst();
        if (target.isEmpty()) {
            throw new IllegalStateException("Could not identify a node to be the re-assignment target");
        }

        Utils.awaitReassignmentOfKafkaInternalTopicsIfNecessary(
                clusterConfig.getAnonConnectConfigForCluster(buildServerList(id -> getEndpointPair(Listener.ANON, id))), nodeId,
                target.get(), 120, TimeUnit.SECONDS);

        portsAllocator.deallocate(nodeId);

        var s = servers.remove(nodeId);
        s.shutdown();
        s.awaitShutdown();
        ensureDirectoryIsEmpty(getBrokerLogDir(nodeId));
    }

    @Override
    public synchronized void stopNodes(Predicate<Integer> nodeIdPredicate, TerminationStyle terminationStyle) {
        var kafkaServersToStop = servers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> !stoppedServers.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        roleOrderedShutdown(kafkaServersToStop, true);

        stoppedServers.addAll(kafkaServersToStop.keySet());
    }

    @Override
    public synchronized void startNodes(Predicate<Integer> nodeIdPredicate) {
        var kafkaServersToStart = servers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> stoppedServers.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        kafkaServersToStart.forEach((key, value) -> {
            var configHolder = clusterConfig.generateConfigForSpecificNode(this, key);
            var replacement = buildKafkaServer(configHolder);
            tryToStartServerWithRetry(configHolder, replacement);
            servers.put(key, replacement);
            stoppedServers.remove(key);
        });
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public synchronized void close() throws Exception {
        try {
            try {
                roleOrderedShutdown(servers, false);
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

    /**
     * Workaround for <a href="https://issues.apache.org/jira/browse/KAFKA-14287">KAFKA-14287</a>.
     * with kraft, if we don't shut down the controller last, we sometimes see a hang.
     */
    private void roleOrderedShutdown(Map<Integer, Server> servers, boolean await) {
        var justBrokers = servers.entrySet().stream()
                .filter(e -> !portsAllocator.hasRegisteredPort(Listener.CONTROLLER, e.getKey()))
                .map(Map.Entry::getValue)
                .toList();
        justBrokers.forEach(Server::shutdown);
        if (await) {
            justBrokers.forEach(Server::awaitShutdown);
        }

        var controllers = servers.entrySet().stream()
                .filter(e -> portsAllocator.hasRegisteredPort(Listener.CONTROLLER, e.getKey()))
                .map(Map.Entry::getValue).toList();
        controllers.forEach(Server::shutdown);
        if (await) {
            controllers.forEach(Server::awaitShutdown);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void ensureDirectoryIsEmpty(Path path) {
        if (Files.exists(path)) {
            try (var ps = Files.walk(path);
                    var s = ps
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)) {
                s.forEach(File::delete);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error whilst deleting " + path, e);
            }
        }
    }

    @Override
    public synchronized int getNumOfBrokers() {
        return servers.size();
    }

    @Override
    public synchronized Set<Integer> getStoppedBrokers() {
        return Set.copyOf(this.stoppedServers);
    }

    @Override
    public synchronized EndpointPair getEndpointPair(Listener listener, int nodeId) {
        var port = portsAllocator.getPort(listener, nodeId);
        return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port)).connect(new Endpoint("localhost", port)).build();
    }

    private static void trapKafkaSystemExit() {
        Exit.setExitProcedure((statusCode, message) -> {
            LOGGER.log(System.Logger.Level.WARNING, "Kafka tried to exit with statusCode: {0} and message: {1}. Dumping stack to trace whats at fault",
                    statusCode, message, new IllegalStateException(message));
            System.out.println("Avoiding shutdown  with code: " + statusCode + " and message: " + message);
        });
    }

}
