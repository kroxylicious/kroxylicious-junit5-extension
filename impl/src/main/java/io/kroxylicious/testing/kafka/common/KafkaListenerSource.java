/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

/**
 * The interface Kafka endpoints.
 */
public interface KafkaListenerSource {

    /**
     * Enumeration of kafka listeners used by the test harness.
     */
    enum Listener {
        /**
         * used for communications to/from consumers/producers optionally with authentication
         */
        EXTERNAL,
        /**
         * used for communications to/from consumers/producers without authentication primarily for the extension to validate the cluster
         */
        ANON,
        /**
         * used for inter-broker communications (always no auth)
         */
        INTERNAL,
        /**
         * used for inter-broker controller communications (kraft - always no auth)
         */
        CONTROLLER
    }

    /**
     * Gets the kafka listen for the given listener and nodeId.
     *
     * @param listener listener
     * @param nodeId   kafka <code>node.id</code>
     * @return kafka listener.
     */
    KafkaListener getKafkaListener(Listener listener, int nodeId);

}
