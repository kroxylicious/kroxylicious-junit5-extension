/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

/**
 * Thrown when SCRAM user initialization against a Kafka cluster fails.
 */
public class ScramInitializationException extends RuntimeException {
    /**
     * Creates a new exception with the given detail message and cause.
     *
     * @param message the detail message
     * @param e       the cause
     */
    public ScramInitializationException(String message, Exception e) {
        super(message, e);
    }
}
