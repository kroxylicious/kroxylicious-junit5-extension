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
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;

import static java.util.stream.Collectors.toSet;

/**
 * Dynamic Topology that:
 * 1. looks at the user's cluster config and determines what nodes should be in the topology and whether they are controllers/brokers
 * 2. connects the desired topology with some environmental resources like allocated ports to build a configuration for each node
 * 3. manifests the topology using the driver, creating the nodes and tracking their state
 * 4. enables manipulation of the topology, stopping/starting/removing nodes.
 */
public class KafkaTopology implements TopologyConfiguration, KafkaCluster {
    private final KafkaClusterDriver driver;
    private final KafkaEndpoints endpoints;
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

    private KafkaTopology(KafkaClusterDriver driver, KafkaEndpoints endpoints, KafkaClusterConfig config) {
        this.driver = driver;
        this.endpoints = endpoints;
        this.config = config;
        if (!config.isKraftMode()) {
            this.zookeeperConfig = new ZookeeperConfig(endpoints);
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
    public void start() {
        startZookeeperIfRequired();
        nodes.values().stream().sorted(NON_CONTROLLERS_FIRST.reversed()).parallel().forEach(KafkaNode::start);
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                getAnonymousClientConfiguration(), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
    }

    @Override
    public synchronized int addBroker() {
        var first = IntStream.rangeClosed(0, nodes.size()).filter(cand -> !nodes.containsKey(cand)).findFirst();
        if (first.isEmpty()) {
            throw new IllegalStateException("Could not determine new nodeId, existing set " + nodes.keySet());
        }
        var newNodeId = first.getAsInt();
        Set<Role> roles = Set.of(Role.BROKER);
        KafkaNodeConfiguration nodeConfiguration = new KafkaNodeConfiguration(this, newNodeId, roles, config, endpoints);
        KafkaNode newNode = driver.createNode(nodeConfiguration);
        this.nodeConfigurations.put(newNodeId, nodeConfiguration);
        this.nodes.put(newNodeId, newNode);
        newNode.start();
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                getAnonymousClientConfiguration(), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
        return newNode.nodeId();
    }

    @Override
    public synchronized void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException {
        KafkaNode node = get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("nodeId isn't in topology: " + nodeId);
        }
        if (nodes.values().stream().anyMatch(KafkaNode::isStopped)) {
            throw new IllegalStateException("cannot remove nodes while brokers are stopped");
        }
        if (node.configuration().isController()) {
            throw new UnsupportedOperationException("cannot remove kraft controller node");
        }
        var target = nodes.values().stream().filter(n -> n.isBroker() && !n.isStopped() && n.configuration().nodeId() != nodeId).findFirst();
        if (target.isEmpty()) {
            throw new IllegalStateException("Could not identify a node to be the re-assignment target");
        }
        Utils.awaitReassignmentOfKafkaInternalTopicsIfNecessary(
                getAnonymousClientConfiguration(), nodeId,
                target.get().nodeId(), 120, TimeUnit.SECONDS);
        stopNodes(value -> value == nodeId, TerminationStyle.GRACEFUL);
        remove(node);
    }

    @Override
    public void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle) {
        stopNodesAndDelay(nodeIdPredicate, terminationStyle, true);
    }

    /**
     * In some cases we need to sleep after the Kafka Node is shutdown to allow some orphaned resources to expire (the ZK session)
     *
     * @param nodeIdPredicate  nodeIds to stop
     * @param terminationStyle termination style
     * @param delay            whether we should delay or not
     */
    private void stopNodesAndDelay(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle, boolean delay) {
        OptionalInt maxOrphanedResourceLifetimeMillis = nodes.values().stream().sorted(NON_CONTROLLERS_FIRST).filter(node -> !node.isStopped())
                .filter(node -> nodeIdPredicate.test(node.nodeId()))
                .mapToInt(node -> node.stop(terminationStyle)).max();
        if (delay && maxOrphanedResourceLifetimeMillis.isPresent() && maxOrphanedResourceLifetimeMillis.getAsInt() > 0) {
            try {
                Thread.sleep(maxOrphanedResourceLifetimeMillis.getAsInt());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void startNodes(IntPredicate nodeIdPredicate) {
        nodes.values().stream().sorted(NON_CONTROLLERS_FIRST.reversed()).filter(KafkaNode::isStopped)
                .filter(node -> nodeIdPredicate.test(node.nodeId()))
                .forEach(KafkaNode::start);
    }

    @Override
    public void close() throws Exception {
        stopNodesAndDelay(value -> true, TerminationStyle.GRACEFUL, false);
        for (KafkaNode node : nodes.values().stream().sorted(NON_CONTROLLERS_FIRST).toList()) {
            remove(node);
        }
        if (this.zookeeper != null) {
            this.zookeeper.close();
        }
        driver.close();
    }

    @Override
    public int getNumOfBrokers() {
        return (int) nodes.values().stream().filter(KafkaNode::isBroker).count();
    }

    @Override
    public Set<Integer> getStoppedBrokers() {
        return nodes.values().stream().filter(KafkaNode::isStopped).map(KafkaNode::nodeId).collect(toSet());
    }

    @Override
    public String getBootstrapServers() {
        return nodes.values().stream().filter(KafkaNode::isBroker)
                .filter(node -> !node.isStopped())
                .map(node -> node.configuration().getBrokerConnectAddress())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(","));
    }

    @Override
    public String getClusterId() {
        return config.clusterId();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return getConnectConfigForCluster();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String user, String password) {
        return getConnectConfigForCluster(user, password);
    }

    @Override
    public boolean isKraftMode() {
        return this.config.isKraftMode();
    }

    public static KafkaTopology create(KafkaClusterDriver driver, KafkaEndpoints endpoints, KafkaClusterConfig config) {
        KafkaTopology kafkaTopology = new KafkaTopology(driver, endpoints, config);
        Map<Integer, KafkaNodeConfiguration> configs = generateConfigurations(config, endpoints, kafkaTopology);
        kafkaTopology.nodeConfigurations.putAll(configs);
        // createNode requires the topology to have all nodes configurations installed first, so it can ask for all
        // controller node details.
        Map<Integer, KafkaNode> nodes = configs.values().stream()
                .collect(Collectors.toMap(KafkaNodeConfiguration::nodeId, driver::createNode));
        kafkaTopology.nodes.putAll(nodes);
        return kafkaTopology;
    }

    @NotNull
    private static Map<Integer, KafkaNodeConfiguration> generateConfigurations(KafkaClusterConfig config, KafkaEndpoints endpoints, KafkaTopology kafkaTopology) {
        int nodesToCreate = Math.max(config.getBrokersNum(), config.getKraftControllers());
        Stream<KafkaNodeConfiguration> nodeConfigurationStream = IntStream.range(0, nodesToCreate)
                .mapToObj(value -> new IdAndRoles(value, roles(config, value)))
                // note that generation of a configuration may allocate resources like ports to it
                .map(idAndRoles -> getNodeConfiguration(kafkaTopology, endpoints, config, idAndRoles));
        return nodeConfigurationStream.collect(Collectors.toMap(KafkaNodeConfiguration::nodeId, configuration -> configuration));
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

    public void startZookeeperIfRequired() {
        if (zookeeper != null) {
            zookeeper.start();
        }
    }

    public record IdAndRoles(int nodeId, Set<Role> roles) {
    }

    private static KafkaNodeConfiguration getNodeConfiguration(KafkaTopology kafkaTopology, KafkaEndpoints endpoints, KafkaClusterConfig config, IdAndRoles idAndRoles) {
        return new KafkaNodeConfiguration(kafkaTopology, idAndRoles.nodeId, idAndRoles.roles, config, endpoints);
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

    public synchronized Map<String, Object> getAnonymousClientConfiguration() {
        String bootstrapServers = nodeDescriptions().stream()
                .filter(KafkaNodeConfiguration::isBroker)
                .map(KafkaNodeConfiguration::getAnonymousConnectAddress)
                .collect(Collectors.joining(","));
        return config.getAnonConnectConfigForCluster(bootstrapServers);
    }

    public synchronized void remove(KafkaNode node) {
        Objects.requireNonNull(node);
        if (!node.isStopped()) {
            throw new RuntimeException("attempting to remove non-stopped node");
        }
        nodes.remove(node.nodeId());
        nodeConfigurations.remove(node.nodeId());
        driver.nodeRemoved(node);
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
