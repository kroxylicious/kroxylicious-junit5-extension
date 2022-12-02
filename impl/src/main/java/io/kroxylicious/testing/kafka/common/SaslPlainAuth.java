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
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to use
 * provide a cluster that supports SASL-PLAIN. {@link User @User} annotations can be used to
 * configure the cluster with users.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface SaslPlainAuth {

}
