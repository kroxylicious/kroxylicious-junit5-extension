/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public interface KafkaClusterDriver {
    KafkaNode createNode(KafkaNodeConfiguration node);

    Zookeeper createZookeeper(ZookeeperConfig zookeeperConfig);

    void nodeRemoved(KafkaNode node);

    void close();
}
