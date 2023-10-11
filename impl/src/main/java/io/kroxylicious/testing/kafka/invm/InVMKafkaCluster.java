/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaTopology;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaCluster implements KafkaCluster {

    private final KafkaCluster cluster;

    public InVMKafkaCluster(KafkaClusterConfig config) {
        InVMKafkaClusterDriver driver = new InVMKafkaClusterDriver();
        cluster = KafkaTopology.create(driver, driver, config);
    }

    @Override
    public void start() {
        cluster.start();
    }

    @Override
    public int addBroker() {
        return cluster.addBroker();
    }

    @Override
    public void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException {
        cluster.removeBroker(nodeId);
    }

    @Override
    public void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle) {
        cluster.stopNodes(nodeIdPredicate, terminationStyle);
    }

    @Override
    public void startNodes(IntPredicate nodeIdPredicate) {
        cluster.startNodes(nodeIdPredicate);
    }

    @Override
    public void close() throws Exception {
        cluster.close();
    }

    @Override
    public int getNumOfBrokers() {
        return cluster.getNumOfBrokers();
    }

    @Override
    public Set<Integer> getStoppedBrokers() {
        return cluster.getStoppedBrokers();
    }

    @Override
    public String getBootstrapServers() {
        return cluster.getBootstrapServers();
    }

    @Override
    public String getClusterId() {
        return cluster.getClusterId();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return cluster.getKafkaClientConfiguration();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String user, String password) {
        return cluster.getKafkaClientConfiguration(user, password);
    }
}
