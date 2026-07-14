/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

/**
 * Thrown when a Kafka cluster configuration is invalid or inconsistent.
 */
public class IllegalConfigurationException extends RuntimeException {
    /**
     * Creates a new exception with the given detail message.
     *
     * @param message the detail message
     */
    public IllegalConfigurationException(String message) {
        super(message);
    }
}
