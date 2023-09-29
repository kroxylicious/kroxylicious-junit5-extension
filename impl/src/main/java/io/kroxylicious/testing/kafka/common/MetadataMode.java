/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public enum MetadataMode {
    ZOOKEEPER,
    KRAFT_COMBINED,
    KRAFT_SEPARATE;

    /**
     * @return The total number of Kafka nodes (excludes any ZooKeeper nodes).
     */
    public int numNodes(int numControllers, int numBrokers) {
        return switch (this) {
            case KRAFT_SEPARATE -> numControllers + numBrokers;
            case KRAFT_COMBINED -> Math.max(numControllers, numBrokers);
            case ZOOKEEPER -> numBrokers;
        };
    }
}
