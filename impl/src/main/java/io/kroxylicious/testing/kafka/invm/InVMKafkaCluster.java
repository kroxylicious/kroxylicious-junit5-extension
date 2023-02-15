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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.jetbrains.annotations.NotNull;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.Utils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.KafkaServer;
import kafka.server.Server;
import kafka.tools.StorageTool;
import scala.Option;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

public class InVMKafkaCluster implements KafkaCluster {
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaCluster.class.getName());

    private final KafkaClusterConfig clusterConfig;
    private final Path tempDirectory;
    private final ServerCnxnFactory zooFactory;
    private final ZooKeeperServer zooServer;
    private final List<Server> servers;
    private final KafkaClusterConfig.KafkaEndpoints kafkaEndpoints;

    public InVMKafkaCluster(KafkaClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        try {
            tempDirectory = Files.createTempDirectory("kafka");
            tempDirectory.toFile().deleteOnExit();

            // kraft mode: per-broker: 1 external port + 1 inter-broker port + 1 controller port
            // zk mode: per-cluster: 1 zk port; per-broker: 1 external port + 1 inter-broker port
            var numPorts = clusterConfig.getBrokersNum() * (clusterConfig.isKraftMode() ? 3 : 2) + (clusterConfig.isKraftMode() ? 0 : 1);
            LinkedList<Integer> ports = Utils.preAllocateListeningPorts(numPorts).collect(Collectors.toCollection(LinkedList::new));

            final Supplier<KafkaClusterConfig.KafkaEndpoints.Endpoint> zookeeperEndpointSupplier;
            if (!clusterConfig.isKraftMode()) {
                var zookeeperPort = ports.pop();

                zooFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", zookeeperPort), 1024);

                var zoo = tempDirectory.resolve("zoo");
                var snapshotDir = zoo.resolve("snapshot");
                var logDir = zoo.resolve("log");
                snapshotDir.toFile().mkdirs();
                logDir.toFile().mkdirs();

                zooServer = new ZooKeeperServer(snapshotDir.toFile(), logDir.toFile(), 500);
                zookeeperEndpointSupplier = () -> new KafkaClusterConfig.KafkaEndpoints.Endpoint("localhost", zookeeperPort);
            }
            else {
                zooFactory = null;
                zooServer = null;
                zookeeperEndpointSupplier = null;
            }
            kafkaEndpoints = new KafkaClusterConfig.KafkaEndpoints() {
                final List<Integer> clientPorts = ports.subList(0, clusterConfig.getBrokersNum());
                final List<Integer> interBrokerPorts = ports.subList(clusterConfig.getBrokersNum(), 2 * clusterConfig.getBrokersNum());
                final List<Integer> controllerPorts = ports.subList(clusterConfig.getBrokersNum() * 2, ports.size());

                @Override
                public EndpointPair getClientEndpoint(int brokerId) {
                    var port = clientPorts.get(brokerId);
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port)).connect(new Endpoint("localhost", port)).build();
                }

                @Override
                public EndpointPair getInterBrokerEndpoint(int brokerId) {
                    var port = interBrokerPorts.get(brokerId);
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port)).connect(new Endpoint("localhost", port)).build();
                }

                @Override
                public EndpointPair getControllerEndpoint(int brokerId) {
                    if (!clusterConfig.isKraftMode()) {
                        throw new IllegalStateException();
                    }
                    var port = controllerPorts.get(brokerId);
                    return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port)).connect(new Endpoint("localhost", port)).build();
                }
            };
            Supplier<KafkaClusterConfig.KafkaEndpoints> kafkaEndpointsSupplier = () -> kafkaEndpoints;

            servers = clusterConfig.getBrokerConfigs(kafkaEndpointsSupplier, zookeeperEndpointSupplier).map(this::buildKafkaServer).collect(Collectors.toList());

        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
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
        LOGGER.log(System.Logger.Level.WARNING, "Generated config {0}", properties);
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

        servers.stream().parallel().forEach(Server::startup);
        // TODO expose timeout. Annotations? Provisioning Strategy duration?
        final String bootstrapServers = clusterConfig.buildClientBootstrapServers(kafkaEndpoints);
        Utils.ensureExpectedBrokerCountInCluster(clusterConfig.getConnectConfigForCluster(bootstrapServers), 120, TimeUnit.SECONDS, clusterConfig.getBrokersNum());

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
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers(),
                user, password);
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
            if (tempDirectory.toFile().exists()) {
                try (var s = Files.walk(tempDirectory)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)) {
                    s.forEach(File::delete);
                }
            }
        }
    }
}
