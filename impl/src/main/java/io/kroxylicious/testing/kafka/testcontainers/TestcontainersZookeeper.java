/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.time.Duration;

import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.common.Zookeeper;

public class TestcontainersZookeeper implements Zookeeper {

    TestcontainersKafkaClusterDriver.ZookeeperContainer zookeeper;

    // If Zookeeper runs for less than 500ms, there's almost certainly a problem. This makes it be treated
    // as a startup failure.
    private static final Duration MINIMUM_RUNNING_DURATION = Duration.ofMillis(500);
    private static final int CONTAINER_STARTUP_ATTEMPTS = 3;

    public TestcontainersZookeeper(DockerImageName imageName, String name, Network network) {
        zookeeper = new TestcontainersKafkaClusterDriver.ZookeeperContainer(imageName)
                .withName(name)
                .withNetwork(network)
                .withMinimumRunningDuration(MINIMUM_RUNNING_DURATION)
                .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                .withNetworkAliases("zookeeper");
    }

    @Override
    public void close() {
        zookeeper.close();
    }

    @Override
    public void start() {
        zookeeper.start();
    }
}
