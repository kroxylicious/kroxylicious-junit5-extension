/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

/**
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to use
 * provide a cluster that supports SASL-PLAIN configured with the
 * given users.
 *
 * @deprecated use @{@link SaslMechanism.Principal} instead.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SaslPlainAuth.List.class)
@KafkaClusterConstraint
@Deprecated(since = "0.9.0", forRemoval = true)
public @interface SaslPlainAuth {

    String user();

    String password();

    /**
     * The interface User password.
     */
    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    @KafkaClusterConstraint
    @Deprecated(since = "0.9.0", forRemoval = true)
    @interface List {
        SaslPlainAuth[] value();
    }
}
