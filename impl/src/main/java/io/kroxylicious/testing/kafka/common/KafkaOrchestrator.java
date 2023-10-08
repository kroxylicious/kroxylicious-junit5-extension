/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;

/**
 * Orchestrates operation of a Kafka Cluster. Responsible for:
 * 1. controlling the order of operations, for example ensuring that non-brokers are stopped first.
 * 2. connecting the selected driver with the users desired cluster configuration
 */
public class KafkaOrchestrator implements KafkaCluster {

    private final KafkaDriver driver;
    private final KafkaTopology kafkaTopology;

    public KafkaOrchestrator(KafkaClusterConfig config, KafkaDriver driver) {
        this.driver = driver;
        kafkaTopology = KafkaTopology.create(driver, config);
    }

    @Override
    public void start() {
        kafkaTopology.startZookeeperIfRequired();
        kafkaTopology.pureBrokersLast().parallel().forEach(KafkaNode::start);
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                kafkaTopology.getAnonymousClientConfiguration(), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
    }

    @Override
    public int addBroker() {
        KafkaNode node = kafkaTopology.addBroker();
        node.start();
        Utils.awaitExpectedBrokerCountInClusterViaTopic(
                kafkaTopology.getAnonymousClientConfiguration(), 120,
                TimeUnit.SECONDS,
                getNumOfBrokers());
        return node.nodeId();
    }

    @Override
    public void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException {
        KafkaNode node = kafkaTopology.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("nodeId isn't in topology: " + nodeId);
        }
        if (kafkaTopology.nodes().anyMatch(KafkaNode::isStopped)) {
            throw new IllegalStateException("cannot remove nodes while brokers are stopped");
        }
        if (node.configuration().isController()) {
            throw new UnsupportedOperationException("cannot remove kraft controller node");
        }
        var target = kafkaTopology.nodes().filter(n -> n.isBroker() && !n.isStopped() && n.configuration().nodeId() != nodeId).findFirst();
        if (target.isEmpty()) {
            throw new IllegalStateException("Could not identify a node to be the re-assignment target");
        }
        Utils.awaitReassignmentOfKafkaInternalTopicsIfNecessary(
                kafkaTopology.getAnonymousClientConfiguration(), nodeId,
                target.get().nodeId(), 120, TimeUnit.SECONDS);
        stopNodes(value -> value == nodeId, TerminationStyle.GRACEFUL);
        kafkaTopology.remove(node);
    }

    @Override
    public void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle) {
        stopNodesAndDelay(nodeIdPredicate, terminationStyle, true);
    }

    /**
     * In some cases we need to sleep after the Kafka Node is shutdown to allow some orphaned resources to expire (the ZK session)
     * @param nodeIdPredicate nodeIds to stop
     * @param terminationStyle termination style
     * @param delay whether we should delay or not
     */
    private void stopNodesAndDelay(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle, boolean delay) {
        OptionalInt maxOrphanedResourceLifetimeMillis = kafkaTopology.pureBrokersFirst().filter(node -> !node.isStopped())
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
        kafkaTopology.pureBrokersLast().filter(KafkaNode::isStopped)
                .filter(node -> nodeIdPredicate.test(node.nodeId()))
                .forEach(KafkaNode::start);
    }

    @Override
    public void close() throws Exception {
        stopNodesAndDelay(value -> true, TerminationStyle.GRACEFUL, false);
        for (KafkaNode node : kafkaTopology.pureBrokersFirst().toList()) {
            kafkaTopology.remove(node);
        }
        kafkaTopology.close();
        driver.close();
    }

    @Override
    public int getNumOfBrokers() {
        return kafkaTopology.getNumBrokers();
    }

    @Override
    public Set<Integer> getStoppedBrokers() {
        return kafkaTopology.getStoppedBrokers();
    }

    @Override
    public String getBootstrapServers() {
        return kafkaTopology.getBootstrapServers();
    }

    @Override
    public String getClusterId() {
        return kafkaTopology.clusterId();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return kafkaTopology.getConnectConfigForCluster();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String user, String password) {
        return kafkaTopology.getConnectConfigForCluster(user, password);
    }
}
