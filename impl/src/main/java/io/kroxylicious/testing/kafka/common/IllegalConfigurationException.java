/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public class IllegalConfigurationException extends RuntimeException {
    public IllegalConfigurationException(String message) {
        super(message);
    }
}
