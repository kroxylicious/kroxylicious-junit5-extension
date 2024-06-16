/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

/**
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to
 * provide a cluster with an external listener configured to expect the given
 * SASL mechanism.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface SaslMechanism {
    String value();
}
