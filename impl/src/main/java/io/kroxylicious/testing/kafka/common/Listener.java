/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

/**
 * Enumeration of kafka listeners used by the test harness.
 */
public enum Listener {
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
