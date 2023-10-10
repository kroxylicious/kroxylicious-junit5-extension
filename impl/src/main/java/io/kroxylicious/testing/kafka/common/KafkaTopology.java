/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toSet;

/**
 * Dynamic Topology that:
 * 1. looks at the user's cluster config and determines what nodes should be in the topology and whether they are controllers/brokers
 * 2. connects the desired topology with some environmental resources like allocated ports to build a configuration for each node
 * 3. manifests the topology using the driver, creating the nodes and tracking their state
 * 4. enables manipulation of the topology, stopping/starting/removing nodes.
 */
public class KafkaTopology implements TopologyConfiguration {
    private final KafkaDriver driver;
    private final KafkaClusterConfig config;

    // kludge: creating a KafkaNode may consult the NodeConfigurations of the Topology looking for controllers, so we
    // need the cluster configuration to be available and associated with the topoloogy for all nodes before we
    // construct any KafkaNodes
    private final Map<Integer, KafkaNodeConfiguration> nodeConfigurations;
    private final Map<Integer, KafkaNode> nodes;
    private final ZookeeperConfig zookeeperConfig;

    private static final Comparator<KafkaNode> NON_CONTROLLERS_FIRST = (o1, o2) -> {
        if (!o1.configuration().isController()) {
            return -1;
        }
        else if (!o2.configuration().isController()) {
            return 1;
        }
        else {
            return 0;
        }
    };
    private final Zookeeper zookeeper;

    private KafkaTopology(KafkaDriver driver, KafkaClusterConfig config) {
        this.driver = driver;
        this.config = config;
        if (!config.isKraftMode()) {
            this.zookeeperConfig = new ZookeeperConfig(driver);
            this.zookeeper = driver.createZookeeper(zookeeperConfig);
        }
        else {
            this.zookeeperConfig = null;
            this.zookeeper = null;
        }
        this.nodes = new HashMap<>();
        this.nodeConfigurations = new HashMap<>();
    }

    @Override
    public boolean isKraftMode() {
        return this.config.isKraftMode();
    }

    public static KafkaTopology create(KafkaDriver driver, KafkaClusterConfig config) {
        KafkaTopology kafkaTopology = new KafkaTopology(driver, config);
        Map<Integer, KafkaNodeConfiguration> configs = generateConfigurations(driver, config, kafkaTopology);
        kafkaTopology.nodeConfigurations.putAll(configs);
        // createNode requires the topology to have all nodes configurations installed first, so it can ask for all
        // controller node details.
        Map<Integer, KafkaNode> nodes = configs.values().stream()
                .collect(Collectors.toMap(KafkaNodeConfiguration::nodeId, driver::createNode));
        kafkaTopology.addNodes(nodes);
        return kafkaTopology;
    }

    @NotNull
    private static Map<Integer, KafkaNodeConfiguration> generateConfigurations(KafkaDriver driver, KafkaClusterConfig config, KafkaTopology kafkaTopology) {
        int nodesToCreate = Math.max(config.getBrokersNum(), config.getKraftControllers());
        Stream<KafkaNodeConfiguration> nodeConfigurationStream = IntStream.range(0, nodesToCreate)
                .mapToObj(value -> new IdAndRoles(roles(config, value), value))
                // note that generation of a configuration may allocate resources like ports to it
                .map(idAndRoles -> getNodeConfiguration(kafkaTopology, config, idAndRoles, driver));
        return nodeConfigurationStream.collect(Collectors.toMap(KafkaNodeConfiguration::nodeId, configuration -> configuration));
    }

    private void addNodes(Map<Integer, KafkaNode> nodes) {
        this.nodes.putAll(nodes);
    }

    @Override
    public String getQuorumVoters() {
        String collect = nodeDescriptions().stream().map(KafkaNodeConfiguration::controllerQuorumAddress)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(","));
        if (collect.isBlank()) {
            throw new IllegalStateException("quorum voters empty! we should have some controllers in the cluster");
        }
        return collect;
    }

    public void close() {
        if (this.zookeeper != null) {
            this.zookeeper.close();
        }
    }

    public void startZookeeperIfRequired() {
        if (zookeeper != null) {
            zookeeper.start();
        }
    }

    public record IdAndRoles(Set<Role> roles, int nodeId) {
    }

    @NotNull
    private static KafkaNodeConfiguration getNodeConfiguration(KafkaTopology kafkaTopology, KafkaClusterConfig config, IdAndRoles idAndRoles, KafkaDriver driver) {
        return new KafkaNodeConfiguration(kafkaTopology, idAndRoles.nodeId, idAndRoles.roles, config, driver);
    }

    private static Set<Role> roles(KafkaClusterConfig config, int nodeId) {
        if (!config.isKraftMode()) {
            return Set.of(Role.BROKER);
        }
        else if (nodeId < config.getBrokersNum() && nodeId < config.getKraftControllers()) {
            return Set.of(Role.BROKER, Role.CONTROLLER);
        }
        else if (nodeId < config.getBrokersNum()) {
            return Set.of(Role.BROKER);
        }
        else {
            return Set.of(Role.CONTROLLER);
        }
    }

    private List<KafkaNodeConfiguration> nodeDescriptions() {
        return nodeConfigurations.values().stream().toList();
    }

    public synchronized KafkaNode addBroker() {
        var first = IntStream.rangeClosed(0, nodes.size()).filter(cand -> !nodes.containsKey(cand)).findFirst();
        if (first.isEmpty()) {
            throw new IllegalStateException("Could not determine new nodeId, existing set " + nodes.keySet());
        }
        var newNodeId = first.getAsInt();
        Set<Role> roles = Set.of(Role.BROKER);
        KafkaNodeConfiguration nodeConfiguration = new KafkaNodeConfiguration(this, newNodeId, roles, config, driver);
        KafkaNode node = driver.createNode(nodeConfiguration);
        this.nodeConfigurations.put(newNodeId, nodeConfiguration);
        this.nodes.put(newNodeId, node);
        return node;
    }

    @NotNull
    public Stream<KafkaNode> pureBrokersFirst() {
        return nodes.values().stream().sorted(NON_CONTROLLERS_FIRST);
    }

    @NotNull
    public Stream<KafkaNode> pureBrokersLast() {
        return nodes.values().stream().sorted(NON_CONTROLLERS_FIRST.reversed());
    }

    @NotNull
    public Stream<KafkaNode> nodes() {
        return nodes.values().stream();
    }

    public synchronized Map<String, Object> getAnonymousClientConfiguration() {
        String bootstrapServers = nodeDescriptions().stream()
                .filter(KafkaNodeConfiguration::isBroker)
                .map(KafkaNodeConfiguration::getAnonymousConnectAddress)
                .collect(Collectors.joining(","));
        return config.getAnonConnectConfigForCluster(bootstrapServers);
    }

    public synchronized void remove(KafkaNode node) {
        Objects.requireNonNull(node);
        doRemove(node);
    }

    private void doRemove(KafkaNode node) {
        Objects.requireNonNull(node);
        if (!node.isStopped()) {
            throw new RuntimeException("attempting to remove non-stopped node");
        }
        nodes.remove(node.nodeId());
        nodeConfigurations.remove(node.nodeId());
        driver.nodeRemoved(node);
    }

    public int getNumBrokers() {
        return (int) nodes.values().stream().filter(KafkaNode::isBroker).count();
    }

    public Set<Integer> getStoppedBrokers() {
        return nodes.values().stream().filter(KafkaNode::isStopped).map(KafkaNode::nodeId).collect(toSet());
    }

    public String getBootstrapServers() {
        return nodes.values().stream().filter(KafkaNode::isBroker)
                .filter(node -> !node.isStopped())
                .map(node -> node.configuration().getBrokerConnectAddress())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(","));
    }

    public String clusterId() {
        return config.clusterId();
    }

    public Map<String, Object> getConnectConfigForCluster() {
        return config.getConnectConfigForCluster(getBootstrapServers());
    }

    public Map<String, Object> getConnectConfigForCluster(String user, String password) {
        return config.getConnectConfigForCluster(getBootstrapServers(), user, password);
    }

    public KafkaNode get(int nodeId) {
        return nodes.get(nodeId);
    }

    @Override
    public Optional<ZookeeperConfig> getZookeeperConfig() {
        return Optional.ofNullable(zookeeperConfig);
    }
}
