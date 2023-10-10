/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaTopology;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class TestcontainersKafkaCluster implements KafkaCluster {

    private final KafkaCluster cluster;
    private final TestcontainersKafkaDriver driver;

    public TestcontainersKafkaCluster(DockerImageName kafka, DockerImageName zookeeper, KafkaClusterConfig config) {
        driver = new TestcontainersKafkaDriver(config.getTestInfo(), config.isKraftMode(), config.getKafkaVersion(), kafka, zookeeper);
        cluster = KafkaTopology.create(driver, config);
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

    public String getKafkaVersion() {
        return driver.getKafkaVersion();
    }
}
