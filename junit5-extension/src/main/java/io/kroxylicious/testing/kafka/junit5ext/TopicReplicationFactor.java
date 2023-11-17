/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;

/**
 * Used to specify the replication factor of a {@link Topic} injected into
 * a test as field or parameter by the KafkaClusterExtension.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@KafkaClusterConstraint
public @interface TopicReplicationFactor {
    /**
     * The topic replication factor.
     *
     * @return topic replication factor.
     */
    short value();
}
