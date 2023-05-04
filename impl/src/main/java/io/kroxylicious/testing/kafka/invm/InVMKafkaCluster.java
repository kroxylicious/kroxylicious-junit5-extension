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
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaException;
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
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.ListeningSocketPreallocator;
import io.kroxylicious.testing.kafka.common.Utils;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;
import static org.awaitility.Awaitility.await;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaCluster implements KafkaCluster {
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaCluster.class.getName());
    private static final int STARTUP_TIMEOUT = 30;

    private final KafkaClusterConfig clusterConfig;
    private final Path tempDirectory;
    private final ServerCnxnFactory zooFactory;
    private final ZooKeeperServer zooServer;
    private final List<Server> servers;
    private final KafkaClusterConfig.KafkaEndpoints kafkaEndpoints;
    private final List<ServerSocket> externalPorts;
    private final List<ServerSocket> anonPorts;
    private final List<ServerSocket> interBrokerPorts;
    private final List<ServerSocket> controllerPorts;

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

            // kraft mode: per-broker: 1 external port + 1 inter-broker port + 1 controller port + 1 anon port
            // zk mode: per-cluster: 1 zk port; per-broker: 1 external port + 1 inter-broker port + 1 anon port
            try (var preallocator = new ListeningSocketPreallocator()) {
                externalPorts = preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()).toList();
                anonPorts = preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()).toList();
                interBrokerPorts = preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()).toList();
                controllerPorts = allocateControllerPorts(clusterConfig, preallocator);
            }

            if (!clusterConfig.isKraftMode()) {
                final Integer zookeeperPort = controllerPorts.get(0).getLocalPort();
                zooFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", zookeeperPort), 1024);

                var zoo = tempDirectory.resolve("zoo");
                var snapshotDir = zoo.resolve("snapshot");
                var logDir = zoo.resolve("log");
                snapshotDir.toFile().mkdirs();
                logDir.toFile().mkdirs();

                zooServer = new ZooKeeperServer(snapshotDir.toFile(), logDir.toFile(), 500);
            }
            else {
                zooFactory = null;
                zooServer = null;
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

                private EndpointPair buildEndpointPair(List<ServerSocket> portRange, int brokerId) {
                    var port = portRange.get(brokerId);
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port.getLocalPort())).connect(new Endpoint("localhost", port.getLocalPort())).build();
                }
            };
            Supplier<KafkaClusterConfig.KafkaEndpoints> clusterEndpointSupplier = () -> kafkaEndpoints;

            servers = clusterConfig.getBrokerConfigs(clusterEndpointSupplier).map(this::buildKafkaServer).collect(Collectors.toList());

        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<ServerSocket> allocateControllerPorts(KafkaClusterConfig clusterConfig, ListeningSocketPreallocator preallocator) {
        if (clusterConfig.isKraftMode()) {
            return preallocator.preAllocateListeningSockets(clusterConfig.getBrokersNum()).toList();
        }
        else {
            return preallocator.preAllocateListeningSockets(1).toList();
        }
    }

    @NotNull
    private Server buildKafkaServer(KafkaClusterConfig.ConfigHolder c) {
        KafkaConfig config = buildBrokerConfig(c, tempDirectory);
        Option<String> threadNamePrefix = Option.apply(null);

        boolean kraftMode = clusterConfig.isKraftMode();
        if (kraftMode) {
            var directories = StorageTool.configToLogDirectories(config);
            var clusterId = c.getKafkaKraftClusterId();
            var metaProperties = StorageTool.buildMetadataProperties(clusterId, config);
            StorageTool.formatCommand(System.out, directories, metaProperties, MINIMUM_BOOTSTRAP_VERSION, true);
            return new KafkaRaftServer(config, Time.SYSTEM, threadNamePrefix);
        }
        else {
            return new KafkaServer(config, Time.SYSTEM, threadNamePrefix, false);
        }
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
    public void start() {
        if (zooFactory != null) {
            try {
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

        servers.stream().parallel().forEach(server -> await().atMost(Duration.ofSeconds(STARTUP_TIMEOUT))
                .catchUncaughtExceptions()
                .ignoreException(KafkaException.class)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    // Hopefully we can remove this once a fix for https://issues.apache.org/jira/browse/KAFKA-14908 actually lands.
                    try {
                        server.startup();
                        return true;
                    }
                    catch (Throwable t) {
                        LOGGER.log(System.Logger.Level.WARNING, "failed to start server due to: " + t.getMessage(), t);
                        server.shutdown();
                        server.awaitShutdown();
                        return false;
                    }
                }));
        Utils.awaitExpectedBrokerCountFromBootstrapServers(clusterConfig.getAnonConnectConfigForCluster(kafkaEndpoints), 120, TimeUnit.SECONDS,
                clusterConfig.getBrokersNum());
    }

    @Override
    public String getClusterId() {
        return clusterConfig.clusterId();
    }

    @Override
    public String getBootstrapServers() {
        return clusterConfig.buildClientBootstrapServers(kafkaEndpoints);
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
    public void close() throws Exception {
        try {
            try {
                servers.stream().parallel().forEach(Server::shutdown);
            }
            finally {
                if (zooServer != null) {
                    zooServer.shutdown(true);
                }
            }
        }
        finally {
            releaseAllPorts();
            if (tempDirectory.toFile().exists()) {
                try (var s = Files.walk(tempDirectory)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)) {
                    s.forEach(File::delete);
                }
            }
        }
    }

    @Override
    public int getNumOfBrokers() {
        return clusterConfig.getBrokersNum();
    }

    private void releaseAllPorts() {
        releasePorts(controllerPorts);
        releasePorts(interBrokerPorts);
        releasePorts(anonPorts);
        releasePorts(externalPorts);
    }

    private void releasePorts(List<ServerSocket> ports) {
        ports.forEach(serverSocket -> {
            try {
                serverSocket.close();
            }
            catch (IOException e) {
                LOGGER.log(System.Logger.Level.WARNING, "failed to close socket: {0} due to: {1}", serverSocket, e.getMessage(), e);
            }
        });
    }
}
