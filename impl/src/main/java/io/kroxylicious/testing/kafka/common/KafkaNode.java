/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import io.kroxylicious.testing.kafka.api.TerminationStyle;

public interface KafkaNode {

    /**
     * Stops the node and returns how long in milliseconds any associated resources like zookeeper sessions live for
     * @param terminationStyle termination style
     * @return milliseconds until orphaned resources expire
     */
    int stop(TerminationStyle terminationStyle);

    void start();

    boolean isStopped();

    int nodeId();

    boolean isBroker();

    KafkaNodeConfiguration configuration();
}
