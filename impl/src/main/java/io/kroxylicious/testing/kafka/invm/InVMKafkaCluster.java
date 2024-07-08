/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import edu.umd.cs.findbugs.annotations.NonNull;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.KafkaServer;
import kafka.server.Server;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import scala.Option;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.PortAllocator;
import io.kroxylicious.testing.kafka.common.Utils;
import io.kroxylicious.testing.kafka.internal.AdminSource;

import static kafka.zk.KafkaZkClient.createZkClient;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaCluster implements KafkaCluster, KafkaClusterConfig.KafkaEndpoints, AdminSource {
    static final System.Logger LOGGER = System.getLogger(InVMKafkaCluster.class.getName());
    private static final int STARTUP_TIMEOUT = 30;
    static final String INVM_KAFKA = "invm-kafka";

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

    private static void exitHandler(int statusCode, String message) {
        final IllegalStateException illegalStateException = new IllegalStateException(message);
        LOGGER.log(System.Logger.Level.WARNING, "Kafka tried to exit with statusCode: {0} and message: {1}. Including stacktrace to determine whats at fault",
                statusCode, message, illegalStateException);
    }

    @NonNull
    private Server buildKafkaServer(KafkaClusterConfig.ConfigHolder c, List<UserScramCredentialRecord> scramUsers) {
        var config = buildBrokerConfig(c);
        var threadNamePrefix = Option.<String> apply(null);

        boolean kraftMode = clusterConfig.isKraftMode();
        if (kraftMode) {
            var clusterId = c.getKafkaKraftClusterId();
            KraftLogDirUtil.prepareLogDirsForKraft(clusterId, config, scramUsers);
            return instantiateKraftServer(config, threadNamePrefix);
        }
        else {
            createScramUsersInZookeeper(scramUsers, config);
            return new KafkaServer(config, Time.SYSTEM, threadNamePrefix, false);
        }
    }

    /**
     * We instantiate the KafkaRaftServer reflectively to support running against versions of kafka
     * older than 3.5.0. This is to enable users to downgrade the broker used for embedded testing rather
     * than forcing them to increase their kafka-clients version to 3.5.0 (broker 3.5.0 requires kafka-clients 3.5.0)
     */
    @NonNull
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

    @NonNull
    private KafkaConfig buildBrokerConfig(KafkaClusterConfig.ConfigHolder c) {
        Properties properties = new Properties();
        properties.putAll(c.getProperties());
        var logsDir = getBrokerLogDir(c.getBrokerNum());
        properties.setProperty(KafkaConfig.LogDirProp(), logsDir.toAbsolutePath().toString());
        LOGGER.log(System.Logger.Level.DEBUG, "Generated config {0}", properties);
        return new KafkaConfig(properties);
    }

    @NonNull
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

        var scramUsers = getUserScramCredentialRecords();
        buildAndStartZookeeper();
        clusterConfig.getBrokerConfigs(() -> this).parallel().forEach(configHolder -> {
            var server = buildKafkaServer(configHolder, scramUsers);
            tryToStartServerWithRetry(configHolder, server);
            servers.put(configHolder.getBrokerNum(), server);
        });
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getAnonConnectConfigForCluster(buildBrokerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))), 120,
                TimeUnit.SECONDS,
                clusterConfig.getBrokersNum());
    }

    private void tryToStartServerWithRetry(KafkaClusterConfig.ConfigHolder configHolder, Server server) {
        Utils.awaitCondition(STARTUP_TIMEOUT, TimeUnit.SECONDS)
                .until(() -> {
                    // Hopefully we can remove this once a fix for https://issues.apache.org/jira/browse/KAFKA-14908 actually lands.
                    try {
                        LOGGER.log(System.Logger.Level.DEBUG, "Attempting to start node: {0} with roles: {1}", configHolder.getBrokerNum(),
                                configHolder.getProperties().get("process.roles"));
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
        return buildBrokerList(nodeId -> getEndpointPair(Listener.EXTERNAL, nodeId));
    }

    private synchronized String buildBrokerList(Function<Integer, KafkaClusterConfig.KafkaEndpoints.EndpointPair> endpointFunc) {
        return servers.keySet().stream()
                .filter(this::isBroker)
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
        final Server server = buildKafkaServer(configHolder, List.of());
        tryToStartServerWithRetry(configHolder, server);
        servers.put(newNodeId, server);

        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                clusterConfig.getAnonConnectConfigForCluster(buildBrokerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
        return configHolder.getBrokerNum();
    }

    @Override
    public synchronized void removeBroker(int nodeId) throws IllegalArgumentException, UnsupportedOperationException {
        if (!servers.containsKey(nodeId)) {
            throw new IllegalArgumentException("Broker node " + nodeId + " is not a member of the cluster.");
        }
        if (clusterConfig.isKraftMode() && isController(nodeId)) {
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
                clusterConfig.getAnonConnectConfigForCluster(buildBrokerList(id -> getEndpointPair(Listener.ANON, id))), nodeId,
                target.get(), 120, TimeUnit.SECONDS);

        portsAllocator.deallocate(nodeId);

        var s = servers.remove(nodeId);
        s.shutdown();
        s.awaitShutdown();
        ensureDirectoryIsEmpty(getBrokerLogDir(nodeId));
    }

    @Override
    public synchronized void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle) {
        var kafkaServersToStop = servers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> !stoppedServers.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        roleOrderedShutdown(kafkaServersToStop);

        stoppedServers.addAll(kafkaServersToStop.keySet());
    }

    @Override
    public synchronized void startNodes(IntPredicate nodeIdPredicate) {
        var kafkaServersToStart = servers.entrySet().stream()
                .filter(e -> nodeIdPredicate.test(e.getKey()))
                .filter(e -> stoppedServers.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        kafkaServersToStart.forEach((key, value) -> {
            var configHolder = clusterConfig.generateConfigForSpecificNode(this, key);
            var replacement = buildKafkaServer(configHolder, List.of());
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
                roleOrderedShutdown(servers);
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
    private void roleOrderedShutdown(Map<Integer, Server> servers) {
        // Shutdown servers without a controller port first.
        shutdownServers(servers, e -> !isController(e.getKey()));
        shutdownServers(servers, e -> isController(e.getKey()));
    }

    private boolean isController(Integer key) {
        // TODO this is nasty. We shouldn't need to go via the portAllocator to figure out what a node is
        // But it is at least testing something meaningful about the configuration
        return portsAllocator.hasRegisteredPort(Listener.CONTROLLER, key);
    }

    private boolean isBroker(Integer key) {
        // TODO this is nasty. We shouldn't need to go via the portAllocator to figure out what a node is
        // But it is at least testing something meaningful about the configuration
        return portsAllocator.hasRegisteredPort(Listener.ANON, key);
    }

    @SuppressWarnings("java:S3864") // Stream.peek is being used with caution.
    private void shutdownServers(Map<Integer, Server> servers, Predicate<Map.Entry<Integer, Server>> entryPredicate) {
        var matchingServers = servers.entrySet().stream()
                .filter(entryPredicate)
                .map(Map.Entry::getValue)
                .peek(Server::shutdown)
                .toList();
        matchingServers.forEach(Server::awaitShutdown);
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
        Exit.setExitProcedure(InVMKafkaCluster::exitHandler);
        Exit.setHaltProcedure(InVMKafkaCluster::exitHandler);
    }

    @Override
    public @NonNull Admin createAdmin() {
        return CloseableAdmin.create(clusterConfig.getAnonConnectConfigForCluster(buildBrokerList(nodeId -> getEndpointPair(Listener.ANON, nodeId))));
    }

    @NonNull
    private List<UserScramCredentialRecord> getUserScramCredentialRecords() {
        if (!clusterConfig.isSaslScram() || clusterConfig.getUsers() == null || clusterConfig.getUsers().isEmpty()) {
            return List.of();
        }
        else {
            return ScramUtils.getScramCredentialRecords(clusterConfig.getSaslMechanism(), clusterConfig.getUsers());
        }
    }

    private void createScramUsersInZookeeper(List<UserScramCredentialRecord> parsedCredentials, KafkaConfig config) {
        if (!parsedCredentials.isEmpty()) {
            ZKClientConfig zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config, false);
            try (var zkClient = createZkClient(INVM_KAFKA, Time.SYSTEM, config, zkClientConfig)) {
                var adminZkClient = new AdminZkClient(zkClient, Option.empty());
                var userEntityType = "users";
                disableControllerCheck(zkClient);
                parsedCredentials.forEach(credentials -> {
                    var userConfig = adminZkClient.fetchEntityConfig(userEntityType, credentials.name());
                    var credentialsString = ScramCredentialUtils.credentialToString(ScramUtils.asScramCredential(credentials));

                    userConfig.setProperty(ScramMechanism.fromType(credentials.mechanism()).mechanismName(), credentialsString);
                    adminZkClient.changeConfigs(userEntityType, credentials.name(), userConfig, false);
                });
            }
        }
    }

    //The accessibility hack is tactical for Kafka >=3.7.1 <= 4.0.0
    //The alternative is copying the code that constructs the ZK client see https://github.com/ozangunalp/kafka-native/pull/195/files as an example. Both options suck,
    //this is less code, and thus we hope less likely to bit rot.
    @SuppressWarnings("java:S3011")
    private static void disableControllerCheck(KafkaZkClient zkClient) {
        try {
            final Class<? extends KafkaZkClient> zkClientClass = zkClient.getClass();
            final Field enableEntityConfigControllerCheck = zkClientClass.getDeclaredField("enableEntityConfigControllerCheck");
            enableEntityConfigControllerCheck.setAccessible(true);
            enableEntityConfigControllerCheck.setBoolean(zkClient, false);
        }
        catch (NoSuchFieldException ignored) {
            //presumably we are on kafka <= 3.7.0, so we can move on with life
        }
        catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to make enableEntityConfigControllerCheck accessible on %s so we are unlikely to be able to create SCRAM users", zkClient.getClass());
        }
    }
}
