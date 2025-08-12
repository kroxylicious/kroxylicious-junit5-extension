/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public class ScramInitializationException extends RuntimeException {
    public ScramInitializationException(String message, Exception e) {
        super(message, e);
    }
}
