/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.extension.ParameterResolutionException;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * Exception thrown when injecting a parameter whose owning {@link KafkaCluster} is ambiguous.
 * You can disambiguate the cluster using {@link Name} annotations.
 */
public class AmbiguousKafkaClusterException extends ParameterResolutionException {

    public AmbiguousKafkaClusterException(String message) {
        super(message);
    }
}
