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
import io.kroxylicious.testing.kafka.common.KafkaOrchestrator;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class TestcontainersKafkaCluster implements KafkaCluster {

    private final KafkaOrchestrator kafkaOrchestrator;
    private final KafkaClusterConfig config;
    private final TestcontainersKafkaDriver driver;

    public TestcontainersKafkaCluster(DockerImageName kafka, DockerImageName zookeeper, KafkaClusterConfig config) {
        this.config = config;
        driver = new TestcontainersKafkaDriver(config.getTestInfo(), config.isKraftMode(), config.getKafkaVersion(), kafka, zookeeper);
        kafkaOrchestrator = new KafkaOrchestrator(config, driver);
    }

    @Override
    public void start() {
        kafkaOrchestrator.start();
    }

    @Override
    public int addBroker() {
        return kafkaOrchestrator.addBroker();
    }

    @Override
    public void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException {
        kafkaOrchestrator.removeBroker(nodeId);
    }

    @Override
    public void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle) {
        kafkaOrchestrator.stopNodes(nodeIdPredicate, terminationStyle);
    }

    @Override
    public void startNodes(IntPredicate nodeIdPredicate) {
        kafkaOrchestrator.startNodes(nodeIdPredicate);
    }

    @Override
    public void close() throws Exception {
        kafkaOrchestrator.close();
    }

    @Override
    public int getNumOfBrokers() {
        return kafkaOrchestrator.getNumOfBrokers();
    }

    @Override
    public Set<Integer> getStoppedBrokers() {
        return kafkaOrchestrator.getStoppedBrokers();
    }

    @Override
    public String getBootstrapServers() {
        return kafkaOrchestrator.getBootstrapServers();
    }

    @Override
    public String getClusterId() {
        return kafkaOrchestrator.getClusterId();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return kafkaOrchestrator.getKafkaClientConfiguration();
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String user, String password) {
        return kafkaOrchestrator.getKafkaClientConfiguration(user, password);
    }

    public String getKafkaVersion() {
        return driver.getKafkaVersion();
    }
}
